"""Literature co-occurrence support."""
import json
import logging
import requests
import os
import aio_pika
from collections import defaultdict
from copy import deepcopy

import redis

from functools import partial
from src.util import create_log_entry
from src.operations import sort_results_score, filter_results_top_n, filter_kgraph_orphans, filter_message_top_n
from datetime import datetime
from fastapi import HTTPException
from requests.models import Response
from requests.exceptions import ConnectionError
from asyncio.exceptions import TimeoutError
from reasoner_pydantic import Query,Message, KnowledgeGraph

from src.rules.rules import rules as AMIE_EXPANSIONS

logger = logging.getLogger(__name__)

# declare the directory where the async data files will exist
queue_file_dir = './queue-files'

def examine_query(message):
    """Decides whether the input is an infer. Returns the grouping node"""
    # Currently, we support:
    # queries that are any shape with all lookup edges
    # OR
    # A 1-hop infer query.
    qedges = message.get('message',{}).get('query_graph',{}).get('edges',{})
    n_infer_edges = 0
    for edge_id, edge_properties in qedges.items():
        if edge_properties.get('knowledge_type','lookup') == 'inferred':
            n_infer_edges += 1
    if (n_infer_edges > 1):
        raise Exception("Only a single infer edge is supported",400)
    if (n_infer_edges > 0) and (n_infer_edges < len(qedges)):
        raise Exception("Mixed infer and lookup queries not supported", 400)
    infer = (n_infer_edges == 1)
    if not infer:
        return infer, None, None
    qnodes = message.get('message',{}).get('query_graph',{}).get('nodes',{})
    question_node = None
    answer_node = None
    for qnode_id, qnode in qnodes.items():
        if qnode.get('ids',None) is None:
            answer_node = qnode_id
        else:
            question_node = qnode_id
    if answer_node is None:
        raise Exception("Both nodes of creative edge pinned", 400)
    if question_node is None:
        raise Exception("No nodes of creative edge pinned", 400)
    return infer,question_node,answer_node


async def entry(message, guid, coalesce_type, caller) -> (dict, int):
    """
    Performs a operation that calls numerous services including strider, aragorn-ranker and answer coalesce

    :param message: should be of a TRAPI Message
    :param guid:
    :param coalesce_type: what kind of answer coalesce type should be performed
    :return: the result of the request and the status code
    """

    try:
        infer, question_qnode, answer_qnode = examine_query(message)
    except Exception as e:
        print(e)
        return None,500

    # A map from operations advertised in our x-trapi to functions
    # This is to functions rather than e.g. service urls because we may combine multiple calls into one op.
    #  e.g. our score operation will include both weighting and scoring for now.
    # Also gives us a place to handle function specific logic
    known_operations = {'lookup': partial(lookup, caller=caller, infer=infer, answer_qnode = answer_qnode, question_qnode = question_qnode),
                        'enrich_results': partial(answercoalesce, coalesce_type=coalesce_type),
                        'overlay_connect_knodes': omnicorp,
                        'score': score,
                        'sort_results_score': sort_results_score,
                        'filter_results_top_n': filter_results_top_n,
                        'filter_kgraph_orphans': filter_kgraph_orphans,
                        'filter_message_top_n': filter_message_top_n,
                        'merge_results_by_qnode': merge_results_by_node_op}

    #  TODO: If inference, don't add enrich to the workflow.  We're already grouping in a particular way
    #  We could maybe enrich by the specific output node independent of the rest of the graph, could be interesting.
    # if the workflow is defined in the message use it, otherwise use the default aragorn workflow
    if 'workflow' in message and not (message['workflow'] is None):
        workflow_def = message['workflow']

        # The underlying tools (strider) don't want the workflow element and will 400
        del message['workflow']
    else:
        if infer:
            workflow_def = [{'id': 'lookup'},
                            {'id': 'overlay_connect_knodes'},
                            {'id': 'score'},
                            {'id': 'filter_message_top_n', 'parameters': {'max_results': 5000}}]
        else:
            #TODO: if this is robokop, need to normalize.
            workflow_def = [{'id': 'lookup'},
                            {'id': 'enrich_results', 'parameters': {'max_input_size': 5000}},
                            {'id': 'overlay_connect_knodes'},
                            {'id': 'score'},
                            {'id': 'filter_message_top_n', 'parameters': {'max_results': 5000}}]

    # convert the workflow def into function calls.
    # Raise a 422 if we find one we don't actually know how to do.
    # We told the world what we can do!
    # Workflow will be a list of the functions, and the parameters if there are any
    workflow = []

    for op in workflow_def:
        try:
            workflow.append((known_operations[op['id']], op.get('parameters', {})))
        except KeyError:
            return f"Unknown Operation: {op}", 422

    final_answer, status_code = await run_workflow(message, workflow, guid)

    # return the workflow def so that the caller can see what we did
    final_answer['workflow'] = workflow_def

    # return the answer
    return final_answer, status_code


async def is_end_message(message):
    if message.get('status_communication', {}).get('strider_multiquery_status','running') == 'complete':
        return True
    return False

async def post_async(host_url, query, guid, params=None):
    """
    Post an asynchronous message.

    Note: this method can return either a "request.models.Response " or a "reasoner-pydantic.message.response"

    :param host_url:
    :param query:
    :param guid:
    :param params:
    :return:
    """
    # get the server root path
    callback_host = os.environ.get('CALLBACK_HOST', '/')

    callback_url = f'{callback_host}/callback/{guid}'

    #query can be a single trapi message, or it can be a dict where each value is a trapi message
    # (e.g. for multiquery strider)
    # If we move the callback to the outer for multiquery this gets alittle easier, but we still need to count the
    # queries, so it's not completely easy
    if 'message' in query.keys():

        # set the callback host in the query
        # TODO this should have the trapi endpoint in production
        query['callback'] = callback_url
        num_queries = 1
        # make sure there is a place for the trapi log messages
        if 'logs' not in query:
            query['logs'] = []
    else:
        for qname,individual_query in query.items():
            individual_query['callback'] = callback_url
            if 'logs' not in individual_query:
                individual_query['logs'] = []
        num_queries = len(query)

    # set the debug level
    # TODO: make sure other aragorn friends do this too
    # query['log_level'] = 'DEBUG'

    # Send the query, using the pid for the callback
    if params is None:
        post_response = requests.post(host_url, json=query)
    else:
        post_response = requests.post(host_url, json=query, params=params)

    # check the response status.
    if post_response.status_code != 200:
        # if there is an error this will return a <requests.models.Response> type
        return post_response

    # create the response object
    response = Response()

    #pydantic_message = Message()
    pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes":{}, "edges":{}})
    accumulated_results = []

    # init the status
    response.status_code = 200

    try:
        # get the rabbitmq connection params
        q_username = os.environ.get('QUEUE_USER', 'guest')
        q_password = os.environ.get('QUEUE_PW', 'guest')
        q_host = os.environ.get('QUEUE_HOST', '127.0.0.1')

        # get a connection to the rabbit mq server
        connection = await aio_pika.connect_robust(host=q_host, login=q_username, password=q_password)

        num_responses = 0

        # use the connection to create a queue using the guid
        async with connection:
            # create a channel to the rabbit mq
            channel = await connection.channel()

            # declare the queue using the guid as the key
            queue = await channel.declare_queue(guid, auto_delete=True)

            # wait for the response.  Timeout after 4 hours
            async with queue.iterator(timeout=60*60*4) as queue_iter:
                # wait the for the message
                async for message in queue_iter:
                    async with message.process():
                        #Got 1
                        num_responses += 1
                        logger.debug(f'{guid}: Strider returned {num_responses} out of {num_queries}.')
                        # build the path/file name
                        file_name = message.body.decode()

                        # check to insure file exists
                        if os.path.exists(file_name):
                            # open and save the file saved from the callback
                            with open(file_name, 'r') as f:
                                # load the contents of the data in the file
                                content = bytes(f.read(), 'utf-8')
                            os.remove(file_name)

                            jr = json.loads(content)
                            query = Query.parse_obj(jr)

                            if await is_end_message(query):
                                break

                            pydantic_kgraph.update(query.message.knowledge_graph)
                            accumulated_results += jr['message']['results']
                        else:
                            # file not found
                            raise HTTPException(500, f'{guid}: Async response data file not found.')

        # set the status to indicate success
        response.status_code = 200
        # save the data to the Response object
        query.message.knowledge_graph = pydantic_kgraph
        json_query = query.dict()
        json_query['message']['results'] = accumulated_results
        response._content = bytes(json.dumps(json_query),'utf-8')

        # close the connection to the queue
        await connection.close()

    except TimeoutError as e:
        error_string = f'{guid}: Async query to {host_url} timed out. Carrying on.'
        logger.exception(error_string, e)
        #response.status_code = 598
        # 598 is too harsh. Set the status to indicate (partial) success
        response.status_code = 200
        # save the data to the Response object
        query.message.knowledge_graph = pydantic_kgraph
        json_query = query.dict()
        json_query['message']['results'] = accumulated_results
        response._content = bytes(json.dumps(json_query),'utf-8')
        #And return
        return response
    except Exception as e:
        error_string = f'{guid}: Queue error exception {e} for callback {query["callback"]}'
        logger.exception(error_string, e)
        raise HTTPException(500, error_string)

    # return with the message
    return response

async def subservice_post(name, url, message, guid, asyncquery=False, params=None) -> (dict, int):
    """
    launches a post request, returns the response.

    :param name: name of service
    :param url: the url of the service
    :param message: the message to post to the service
    :param guid: run identifier
    :param asyncquery:
    :param params: the parameters passed to the service
    :return: dict, status code
    """
    # init return values
    ret_val = message

    # are we going to include the timings
    debug = os.environ.get('DEBUG_TIMING', 'False')

    # if we are capturing the timings
    if debug == 'True':
        dt_start = datetime.now()
    else:
        dt_start = None

    # remove the workflow element
    if 'workflow' in message and message['workflow'] is None:
        del message['workflow']

    logger.debug(f"{guid}: Calling {url}")

    try:
        # launch the post depending on the query type and get the response
        if asyncquery:
            # handle the response
            response = await post_async(url, message, guid, params)
        else:
            if params is None:
                response = requests.post(url, json=message)
            else:
                response = requests.post(url, json=message, params=params)

        # save the response code
        status_code = response.status_code

        logger.debug(f'{guid}: {name} returned with {status_code}')

        if status_code == 200:
            try:
                # if there is a response return it as a dict
                if len(response.json()):
                    ret_val = response.json()

            except Exception as e:
                status_code = 500
                logger.exception(f"{guid}: ARAGORN Exception {e} translating json from post to {name}")

    except ConnectionError as ce:
        status_code = 404
        logger.exception(f'{guid}: ARAGORN ConnectionError {ce} posting to {name}')
    except Exception as e:
        status_code = 500
        logger.exception(f"{guid}: ARAGORN Exception {e} posting to {name}")

    # make sure there is a place for the trapi log messages
    if 'logs' not in ret_val:
        ret_val['logs'] = []

    # html error code returned
    if status_code != 200:
        msg = f'{name} HTML error status code {status_code} returned.'

        logger.error(f'{guid}: {msg}')

        ret_val['logs'].append(create_log_entry(msg, "ERROR"))
    # good html status code
    elif len(ret_val['message'].get('results',[])) == 0:
        msg = f'{name} No results returned.'

        logger.warning(f'{guid}: {msg}')

        ret_val['logs'].append(create_log_entry(msg, "WARNING"))
    else:
        logger.info(f'{guid}: {name} returned {len(ret_val["message"]["results"])} results.')

    if debug == 'True':
        diff = datetime.now() - dt_start

        ret_val['logs'].append(create_log_entry(f'End of {name} processing. Time elapsed: {diff.seconds} seconds', 'DEBUG'))

    return ret_val, status_code


async def lookup(message, params, guid, infer=False, caller='ARAGORN', answer_qnode=None, question_qnode=None) -> (dict, int):
    """
    Performs lookup, parameterized by ARAGORN/ROBOKOP and whether the query is an infer type query

    :param message:
    :param params:
    :param guid:
    :param caller:
    :return:
    """

    if caller == 'ARAGORN':
        return await aragorn_lookup(message,params,guid,infer,answer_qnode)
    elif caller == 'ROBOKOP':
        return await robokop_lookup(message,params,guid,infer,question_qnode,answer_qnode)
    return f'Illegal caller {caller}', 400

def chunk(input, n):
    for i in range(0, len(input), n):
        yield input[i:i+n]


async def aragorn_lookup(input_message,params,guid,infer,answer_qnode):
    if not infer:
        return await strider(input_message,params,guid)
    #Now it's an infer query.
    messages = await expand_query(input_message,params,guid)
    nrules_per_batch = int(os.environ.get("MULTISTRIDER_BATCH_SIZE", 100))
    nrules = int(os.environ.get("MAXIMUM_MULTISTRIDER_RULES",len(messages)))
    result_messages = []
    num = 0
    num_batches_returned = 0
    for to_run in chunk(messages[:nrules],nrules_per_batch):
        message={}
        for q in to_run:
            num += 1
            message[f'query_{num}'] = q
        result_message, sc = await multi_strider(message,params,guid)
        num_batches_returned += 1
        logger.info(f'{guid}: {num_batches_returned} batches returned')
        result_messages.append(result_message)
    logger.info(f"{guid}: strider complete")
    #We have to stitch stuff together again
    pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes":{}, "edges":{}})
    for rm in result_messages:
        pydantic_kgraph.update(KnowledgeGraph.parse_obj(rm['message']['knowledge_graph']))
    result = result_messages[0]
    result['message']['knowledge_graph'] = pydantic_kgraph.dict()
    #Now the merged message has the wrong query, let's fix it:
    result['message']['query_graph'] = deepcopy(input_message['message']['query_graph'])
    for rm in result_messages[1:]:
        result['message']['results'].extend( rm['message']['results'])
    mergedresults = await merge_results_by_node(result,answer_qnode)
    logger.info(f'{guid}: results merged')
    return mergedresults, sc

async def merge_results_by_node_op(message,params,guid) -> (dict,int):
    qn = params['merge_qnode']
    merged_results = await merge_results_by_node(message, qn)
    return merged_results,200

async def strider(message,params,guid) -> (dict, int):
    strider_url = os.environ.get("STRIDER_URL", "https://strider.renci.org/1.2/")

    # select the type of query post. "test" will come from the tester
    if 'test' in message:
        strider_url += 'query'
        asyncquery = False
    else:
        strider_url += 'asyncquery'
        asyncquery = True

    response = await subservice_post('strider', strider_url, message, guid, asyncquery=asyncquery)

    return response


async def normalize_qgraph_ids(m):
    url = f'{os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/1.2/")}get_normalized_node'
    qnodes = m['message']['query_graph']['nodes']
    qnode_ids = set()
    for qid, qnode in qnodes.items():
        if 'ids' in qnode:
            qnode_ids.update(qnode['ids'])
    nnp = { "curies": list(qnode_ids), "conflate": True }
    nnresult = requests.post(url,json=nnp)
    if nnresult.status_code == 200:
        for qid, qnode in qnodes.items():
            if 'ids' in qnode:
                new_ids = [ nnresult[i]['id']['identifier'] for i in qnode['ids']]
                qnode['ids'] = new_ids
    return m


async def robokop_lookup(message,params,guid,infer,question_qnode,answer_qnode) -> (dict, int):
    #For robokop, gotta normalize
    message = await normalize_qgraph_ids(message)
    if not infer:
        kg_url = os.environ.get("ROBOKOPKG_URL", "https://automat.renci.org/robokopkg/1.2/")
        return await subservice_post('robokopkg', f'{kg_url}query', message, guid)

    #It's an infer, just look it up
    rokres =  await robokop_infer(message, guid, question_qnode, answer_qnode)
    return rokres
    #return await normalize(rokres,params,guid)


#TODO this is a temp implementation that assumes we will have (something treats identifier) as the query.
async def expand_query(input_message,params,guid):
    #What are the relevant qnodes and ids from the input message?
    for edge_id, edge in input_message['message']['query_graph']['edges'].items():
        input_q_chemical_node = edge['subject']
        input_q_disease_node = edge['object']
    input_disease_id = input_message['message']['query_graph']['nodes'][input_q_disease_node]['ids'][0]
    messages = []
    for rule in AMIE_EXPANSIONS:
        query = rule.substitute(disease=input_q_disease_node, chemical=input_q_chemical_node,
                                disease_id=input_disease_id)
        message = {'message':json.loads(query)}
        messages.append(message)
    return messages

async def multi_strider(messages,params,guid):
    strider_url = os.environ.get("STRIDER_URL", "https://strider-dev.apps.renci.org/1.2/")

    strider_url += 'multiquery'
    response, status_code = await subservice_post('strider', strider_url, messages, guid, asyncquery=True)

    return response, status_code

async def merge_answer(results,qnode_ids):
    #We are going rename most of the node and edge bindings.  But, we want to preserve bindings to things in the
    # original query.  For the current creative one-hop, that is just nodes b/c we are replacing the one edge.
    #The original qnode bindings are the same in all results by construction
    mergedresult = {'node_bindings': { q: results[0]['node_bindings'][q] for q in qnode_ids},
                    'edge_bindings': {}}

    for bindingtype in ['node', 'edge']:
        bound_things = set()
        for result in results:
            # the gross part here is dealing with the lists in the values of the bindings.
            for key,binding in result[f'{bindingtype}_bindings'].items():
                if key in qnode_ids:
                    continue
                bound = frozenset([x['id'] for x in binding])
                if bound not in bound_things:
                    #found a binding to add
                    n = f'_dummy_{bindingtype}_{len(bound_things)}'
                    mergedresult[f'{bindingtype}_bindings'][n] = binding
                    bound_things.add(bound)
    return mergedresult

#TODO move into operations? Make a translator op out of this
async def merge_results_by_node(result_message, merge_qnode):
    """This assumes a single result message, with a single merged KG.  The goal is to take all results that share a
    binding for merge_qnode and combine them into a single result.
    Assumes that the results are not scored."""
    #This is relatively straightforward: group all the results by the merge_qnode
    # for each one, the only complication is in the keys for the "dummy" bindings.
    original_results = result_message['message']['results']
    original_qnodes = result_message['message']['query_graph']['nodes'].keys()
    #group results
    grouped_results = defaultdict(list)
    for result in original_results:
        answer = result['node_bindings'][merge_qnode]
        bound = frozenset([x['id'] for x in answer])
        grouped_results[bound].append(result)
    #TODO : I'm sure there's a better way to handle this with asyncio
    new_results = []
    for r in grouped_results:
        x = await merge_answer(grouped_results[r],original_qnodes)
        new_results.append(x)
    result_message['message']['results'] = new_results
    return result_message


async def robokop_infer(input_message, guid, question_qnode, answer_qnode):
    automat_url = os.environ.get("ROBOKOPKG_URL", "https://automat.renci.org/robokopkg/1.2/query")
    messages = await expand_query(input_message,{},guid)
    result_messages = []
    for message in messages:
        results = requests.post(automat_url,json=message)
        if results.status_code == 200:
            message = results.json()
            if len(message['message']['results']) > 0:
                result_messages.append(message)
    if len(result_messages) > 0:
        # We have to stitch stuff together again
        # Should this somehow be merged with the similar stuff merging from multistrider?  Probably
        pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes": {}, "edges": {}})
        for rm in result_messages:
            pydantic_kgraph.update(KnowledgeGraph.parse_obj(rm['message']['knowledge_graph']))
        result = result_messages[0]
        result['message']['knowledge_graph'] = pydantic_kgraph.dict()
        for rm in result_messages[1:]:
            result['message']['results'].extend(rm['message']['results'])
        mergedresults = await merge_results_by_node(result, answer_qnode)
    else:
        mergedresults = {'message':{'knowledge_graph':{'nodes':{},'edges':{}}, 'results':[]}}
    #The merged results will have some expanded query, we want the original query.
    mergedresults['message']['query_graph'] = input_message['message']['query_graph']
    return mergedresults, 200

async def answercoalesce(message, params, guid, coalesce_type='all') -> (dict, int):
    """
    Calls answercoalesce
    :param message:
    :param params:
    :param guid:
    :param coalesce_type:
    :return:
    """
    url = f'{os.environ.get("ANSWER_COALESCE_URL", "https://answercoalesce.renci.org/1.2/coalesce/")}{coalesce_type}'

    with open('crap.json','w') as outf:
        json.dump(message,outf)

    # With the current answercoalesce, we make the result list longer, and frequently much longer.  If
    # we've already got 10s of thousands of results, let's skip this step...
    if 'max_input_size' in params:
        if len(message['message'].get('results',0)) > params['max_input_size']:
            # This is already too big, don't do anything else
            return message, 200
    return await subservice_post('answer_coalesce', url, message, guid)

async def normalize(message,params, guid) -> (dict,int):
    url = f'{os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/1.2/")}response'
    return await subservice_post('nodenorm', url, message, guid)

async def omnicorp(message, params, guid) -> (dict, int):
    """
    Calls omnicorp
    :param message:
    :param params:
    :param guid:
    :return:
    """
    url = f'{os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/1.2/")}omnicorp_overlay'

    return await subservice_post('omnicorp', url, message, guid)


async def score(message, params, guid) -> (dict, int):
    """
    Calls weight correctness followed by scoring
    :param message:
    :param params:
    :param guid:
    :return:
    """
    ranker_url = os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/1.2/")

    weight_url = f'{ranker_url}weight_correctness'
    message, status_code = await subservice_post('weight', weight_url, message, guid)

    score_url = f'{ranker_url}score'
    return await subservice_post('score', score_url, message, guid)

async def run_workflow(message, workflow, guid) -> (dict, int):
    """

    :param message:
    :param workflow:
    :param guid:
    :return:
    """
    logger.debug(f'{guid}: incoming message: {message}')

    status_code = None

    for operator_function, params in workflow:
        message, status_code = await operator_function(message, params, guid)

        if status_code != 200 or 'results' not in message['message']:
            break
        elif len(message['message']['results']) == 0:
            break

        # loop through all the log entries and fix the timestamps
        if 'logs' in message:
            for item in message['logs']:
                item['timestamp'] = str(item['timestamp'])

    # return the requested data
    return message, status_code


def one_hop_message(curie_a, type_a, type_b, edge_type, reverse=False) -> dict:
    """
    Creates a tests message.
    :param curie_a:
    :param type_a:
    :param type_b:
    :param edge_type:
    :param reverse:
    :return:
    """
    query_graph = {
                    "nodes": [
                        {
                            "id": "a",
                            "type": type_a,
                            "curie": curie_a
                        },
                        {
                            "id": "b",
                            "type": type_b
                        }
                    ],
                    "edges": [
                        {
                            "id": "ab",
                            "source_id": "a",
                            "target_id": "b"
                        }
                    ]
                }

    if edge_type is not None:
        query_graph['edges'][0]['type'] = edge_type

        if reverse:
            query_graph['edges'][0]['source_id'] = 'b'
            query_graph['edges'][0]['target_id'] = 'a'

    message = {
                "message":
                {
                    "query_graph": query_graph,
                    'knowledge_graph': {"nodes": [], "edges": []},
                    'results': []
                }
            }
    return message
