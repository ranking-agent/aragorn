"""Literature co-occurrence support."""
import json
import logging
import asyncio
import httpx
import os
from collections import defaultdict
from copy import deepcopy
from datetime import datetime as dt, timedelta
from string import Template

from functools import partial
from src.util import create_log_entry, get_channel_pool
from src.operations import sort_results_score, filter_results_top_n, filter_kgraph_orphans, filter_message_top_n
from src.process_db import add_item
from datetime import datetime
from requests.models import Response
from requests.exceptions import ConnectionError
from asyncio.exceptions import TimeoutError
from reasoner_pydantic import Query, KnowledgeGraph, QueryGraph
from reasoner_pydantic import Response as PDResponse
import uuid

#from src.rules.rules import rules as AMIE_EXPANSIONS

logger = logging.getLogger(__name__)

# Get rabbitmq channel pool
channel_pool = get_channel_pool()

# declare the directory where the async data files will exist
queue_file_dir = "./queue-files"

#Load in the AMIE rules.  I'm not sure how this works wrt startup and workers.
thisdir = os.path.dirname(__file__)
#Temporarily point to a typed rules file.  In the future, we will get types in the basic rules and use the config
# to generate "rules.json" in the "rules" directory.
#rulefile = os.path.join(thisdir,"rules","rules.json")
rulefile = os.path.join(thisdir,"rules","kara_typed_rules","rules_with_types_cleaned.json")
with open(rulefile,'r') as inf:
    AMIE_EXPANSIONS = json.load(inf)

def examine_query(message):
    """Decides whether the input is an infer. Returns the grouping node"""
    # Currently, we support:
    # queries that are any shape with all lookup edges
    # OR
    # A 1-hop infer query.
    try:
        # this can still fail if the input looks like e.g.:
        #  "query_graph": None
        qedges = message.get("message", {}).get("query_graph", {}).get("edges", {})
    except:
        qedges = {}
    n_infer_edges = 0
    for edge_id in qedges:
        if qedges.get(edge_id, {}).get("knowledge_type", "lookup") == "inferred":
            n_infer_edges += 1
    if n_infer_edges > 1:
        raise Exception("Only a single infer edge is supported", 400)
    if (n_infer_edges > 0) and (n_infer_edges < len(qedges)):
        raise Exception("Mixed infer and lookup queries not supported", 400)
    infer = n_infer_edges == 1
    if not infer:
        return infer, None, None
    qnodes = message.get("message", {}).get("query_graph", {}).get("nodes", {})
    question_node = None
    answer_node = None
    for qnode_id, qnode in qnodes.items():
        if qnode.get("ids", None) is None:
            answer_node = qnode_id
        else:
            question_node = qnode_id
    if answer_node is None:
        raise Exception("Both nodes of creative edge pinned", 400)
    if question_node is None:
        raise Exception("No nodes of creative edge pinned", 400)
    return infer, question_node, answer_node


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
        return None, 500

    # A map from operations advertised in our x-trapi to functions
    # This is to functions rather than e.g. service urls because we may combine multiple calls into one op.
    #  e.g. our score operation will include both weighting and scoring for now.
    # Also gives us a place to handle function specific logic
    known_operations = {
        "lookup": partial(lookup, caller=caller, infer=infer, answer_qnode=answer_qnode, question_qnode=question_qnode),
        "enrich_results": partial(answercoalesce, coalesce_type=coalesce_type),
        "overlay_connect_knodes": omnicorp,
        "score": score,
        "sort_results_score": sort_results_score,
        "filter_results_top_n": filter_results_top_n,
        "filter_kgraph_orphans": filter_kgraph_orphans,
        "filter_message_top_n": filter_message_top_n,
        "merge_results_by_qnode": merge_results_by_node_op,
    }

    #  TODO: If inference, don't add enrich to the workflow.  We're already grouping in a particular way
    #  We could maybe enrich by the specific output node independent of the rest of the graph, could be interesting.
    # if the workflow is defined in the message use it, otherwise use the default aragorn workflow
    if "workflow" in message and not (message["workflow"] is None):
        workflow_def = message["workflow"]

        # The underlying tools (strider) don't want the workflow element and will 400
        del message["workflow"]
    else:
        if infer:
            workflow_def = [
                {"id": "lookup"},
                {"id": "overlay_connect_knodes"},
                {"id": "score"},
                {"id": "filter_message_top_n", "parameters": {"max_results": 500}},
            ]
        else:
            # TODO: if this is robokop, need to normalize.
            workflow_def = [
                {"id": "lookup"},
                #Removing enrich results for the time being; it's not bl3 compliant and we want to replace it
                # with tagging anyway
                #{"id": "enrich_results", "parameters": {"max_input_size": 5000}},
                {"id": "overlay_connect_knodes"},
                {"id": "score"},
                {"id": "filter_message_top_n", "parameters": {"max_results": 5000}},
            ]

    # convert the workflow def into function calls.
    # Raise a 422 if we find one we don't actually know how to do.
    # We told the world what we can do!
    # Workflow will be a list of the functions, and the parameters if there are any
    workflow = []

    for op in workflow_def:
        try:
            workflow.append((known_operations[op["id"]], op.get("parameters", {}), op["id"]))
        except KeyError:
            return f"Unknown Operation: {op}", 422

    final_answer, status_code = await run_workflow(message, workflow, guid)

    # return the workflow def so that the caller can see what we did
    final_answer["workflow"] = workflow_def

    # return the answer
    return final_answer, status_code


def is_end_message(message):
    if message.get("status_communication", {}).get("strider_multiquery_status", "running") == "complete":
        return True
    return False


async def post_with_callback(host_url, query, guid, params=None):
    """
    Post an asynchronous message.

    Put the callback inthe right place, fire the message, and collect and return all the
    async returns with no further processing.

    Note: this method can return either a "request.models.Response " or a "reasoner-pydantic.message.response"

    :param host_url:
    :param query:
    :param guid:
    :param params:
    :return:
    """
    # get the server root path
    callback_host = os.environ.get("CALLBACK_HOST", "/")

    callback_url = f"{callback_host}/callback/{guid}"

    # query can be a single trapi message, or it can be a dict where each value is a trapi message
    # (e.g. for multiquery strider)
    # If we move the callback to the outer for multiquery this gets alittle easier, but we still need to count the
    # queries, so it's not completely easy
    if "message" in query.keys():
        # set the callback host in the query
        query["callback"] = callback_url
        num_queries = 1
        # make sure there is a place for the trapi log messages
        if "logs" not in query:
            query["logs"] = []
    else:
        for qname, individual_query in query.items():
            individual_query["callback"] = callback_url
            if "logs" not in individual_query:
                individual_query["logs"] = []
        num_queries = len(query)

    # Create the callback queue
    await create_queue(guid)

    # Send the query, using the pid for the callback
    try:
        # these requests should be very quick, if the external service is responsive, they should send back a quick
        # response and then we watch the queue. We give a short 1 min timeout.
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=60)) as client:
            if params is None:
                post_response = await client.post(
                    host_url,
                    json=query,
                )
            else:
                post_response = await client.post(
                    host_url,
                    json=query,
                    params=params,
                )
        # check the response status.
        if post_response.status_code != 200:
            # queue isn't needed for failed service call
            logger.warning(f"Deleting unneeded queue for {guid}")
            await delete_queue(guid)
            # if there is an error this will return a <requests.models.Response> type
            return post_response
    except httpx.RequestError as e:
        logger.error(f"Failed to contact {host_url}")
        await delete_queue(guid)
        # exception handled in subservice_post
        raise e

    # async wait for callbacks to come on queue
    responses = await collect_callback_responses(guid, num_queries)
    return responses


async def collect_callback_responses(guid, num_queries):
    """Collect all callback responses.  No parsing"""
    # create the response object
    # response = Response()

    # pydantic_message = Message()
    #pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes": {}, "edges": {}})
    #accumulated_results = []

    # Don't spend any more time than this assembling messages
    OVERALL_TIMEOUT = timedelta(hours=1)  # 1 hour

    done = False
    start = dt.now()

    responses = []
    while not done:
        new_responses, done = await check_for_messages(guid, num_queries, len(responses) )
        responses.extend(new_responses)
        time_spent = dt.now() - start
        if time_spent > OVERALL_TIMEOUT:
            logger.info(f"{guid}: Timing out receiving callbacks")
            done = True

    await delete_queue(guid)

    # set the status to indicate success
    #response.status_code = 200
    # save the data to the Response object
    #query = Query.parse_obj({"message": {}})
    #query.message.knowledge_graph = pydantic_kgraph
    #json_query = query.dict()
    #json_query["message"]["results"] = accumulated_results
    #response._content = bytes(json.dumps(json_query), "utf-8")

    return responses


async def create_queue(guid):
    try:
        async with channel_pool.acquire() as channel:
            # declare the queue using the guid as the key
            queue = await channel.declare_queue(guid)
    except Exception as e:
        logger.error(f"{guid}: Failed to create queue.")
        raise e


async def delete_queue(guid):
    try:
        async with channel_pool.acquire() as channel:
            # declare the queue using the guid as the key
            queue = await channel.queue_delete(guid)
    except Exception:
        logger.error(f"{guid}: Failed to delete queue.")
        # Deleting queue isn't essential, so we will continue

def has_unique_nodes(result):
    """Given a result, return True if all nodes are unique, False otherwise"""
    seen = set()
    for qnode, knodes in result["node_bindings"].items():
        knode_ids = frozenset([knode["id"] for knode in knodes])
        if knode_ids in seen:
            return False
        seen.add(knode_ids)
    return True

async def filter_repeated_nodes(response,guid):
    """We have some rules that include e.g. 2 chemicals.   We don't want responses in which those two
    are the same.   If you have A-B-A-C then what shows up in the ui is B-A-C which makes no sense."""
    original_result_count = len(response["message"]["results"])
    results = list(filter( lambda x: has_unique_nodes(x), response["message"]["results"] ))
    response["message"]["results"] = results
    if len(results) != original_result_count:
        await filter_kgraph_orphans(response,{},guid)

async def check_for_messages(guid, num_queries, num_previously_received=0):
    """Check for messages on the queue.  Return a list of responses.  This does not care if these
    are TRAPI or anything else.  It does need to know whether to expect more than 1 query.
    Mostly the num_queries and num_received are there for logging."""
    complete = False
    # We want to reset the connection to rabbit every once in a while
    # The timeout is how long to wait for a next message after processing.  So when there are many messages
    # coming in, the connection will stay open for longer than this time
    responses = []
    CONNECTION_TIMEOUT = 1 * 60  # 1 minutes
    num_responses = num_previously_received
    try:
        async with channel_pool.acquire() as channel:
            queue = await channel.get_queue(guid, ensure=True)
            # wait for the response.  Timeout after
            async with queue.iterator(timeout=CONNECTION_TIMEOUT) as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        num_responses += 1
                        logger.info(f"{guid}: Strider returned {num_responses} out of {num_queries}.")
                        jr = process_message(message)
                        if is_end_message(jr):
                            logger.info(f"{guid}: Received complete message from multistrider")
                            complete = True
                            break

                        # it's a real message; update the kgraph and results
                        #await filter_repeated_nodes(jr,guid)
                        #query = Query.parse_obj(jr)
                        #pydantic_kgraph.update(query.message.knowledge_graph)
                        #if jr["message"]["results"] is None:
                        #    jr["message"]["results"] = []
                        #accumulated_results += jr["message"]["results"]
                        responses.append(jr)
                        logger.info(f"{guid}: {len(jr['message']['results'])} results from {jr['message']['query_graph']}")

                        # this is a little messy because this is trying to handle multiquery (returns an end message)
                        # and single query (no end message; single query)
                        if num_queries == 1:
                            logger.info(f"{guid}: Single message returned from strider; continuing")
                            complete = True
                            break

    except TimeoutError as e:
        logger.debug(f"{guid}: cycling aio_pika connection")
    except Exception as e:
        logger.error(f"{guid}: Exception {e}. Returning {num_responses} results we have so far.")
        return responses, True

    return responses, complete


def process_message(message):
    file_name = message.body.decode()
    # open and save the file saved from the callback
    with open(file_name, "r") as f:
        # load the contents of the data in the file
        content = bytes(f.read(), "utf-8")
    os.remove(file_name)
    jr = json.loads(content)
    return jr

#There's a problem where our pydantic model includes a datetime.  But that doesn't serialize to json.
# So when we pass a response through pydantic to remove nulls, it converts log datetimes into python
# datetimes, which then barf when we try to json serialize them.
# This is a frequent complain re: pydantic. See https://github.com/pydantic/pydantic/issues/1409
# Apparently it will be handled in v2, real soon now.  But for the time being, the following code
# from that thread takes the output from .dict() and makes it serializable.
import pydantic.json
# https://github.com/python/cpython/blob/7b21108445969398f6d1db9234fc0fe727565d2e/Lib/json/encoder.py#L78
JSONABLE_TYPES = (dict, list, tuple, str, int, float, bool, type(None))
async def to_jsonable_dict(obj):
    if isinstance(obj, dict):
        return {key: await to_jsonable_dict(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [await to_jsonable_dict(value) for value in obj]
    elif isinstance(obj, tuple):
        return tuple(await to_jsonable_dict(value) for value in obj)
    elif isinstance(obj, JSONABLE_TYPES):
        return obj
    return pydantic.json.pydantic_encoder(obj)
####

async def subservice_post(name, url, message, guid, asyncquery=False, params=None) -> (dict, int):
    """
    launches a post request, returns the response.
    Assumes that the input and output are TRAPI.  This means that multistrider shouldn't go through here.`

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
    debug = os.environ.get("DEBUG_TIMING", "False")

    # if we are capturing the timings
    if debug == "True":
        dt_start = datetime.now()
    else:
        dt_start = None

    # remove the workflow element
    if "workflow" in message and message["workflow"] is None:
        del message["workflow"]

    logger.info(f"{guid}: Calling {url}")

    try:
        # launch the post depending on the query type and get the response
        if asyncquery:
            # handle the response
            responses = await post_with_callback(url, message, guid, params)
            #post_with_callback is returning the json dict, not a response, but the stuff below expects
            # response object
            response = Response()
            response.status_code = 200
            response._content = bytes(json.dumps(responses[0]), "utf-8")
        else:
            # async call to external services with hour timeout
            async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=60 * 60)) as client:
                if params is None:
                    response = await client.post(
                        url,
                        json=message,
                    )
                else:
                    response = await client.post(
                        url,
                        json=message,
                        params=params,
                    )

        # save the response code
        status_code = response.status_code

        logger.debug(f"{guid}: {name} returned with {status_code}")

        if status_code == 200:
            try:
                # if there is a response return it as a dict
                if len(response.json()):
                    #pass it through pydantic for validation and cleaning
                    ret_val = await to_jsonable_dict(PDResponse.parse_obj(response.json()).dict(exclude_none = True))

            except Exception as e:
                status_code = 500
                logger.exception(f"{guid}: ARAGORN Exception {e} translating json from post to {name}")
        elif status_code == 422:
            logger.exception(f"{guid}: {name} was sent an invalid message: {message}")
            logger.exception(response.json())
            logger.exception(response)

    except ConnectionError as ce:
        status_code = 404
        logger.exception(f"{guid}: ARAGORN ConnectionError {ce} posting to {name}")
    except Exception as e:
        status_code = 500
        logger.exception(f"{guid}: ARAGORN Exception {e} posting to {name}")

    if "message" not in ret_val:
        # this mainly handles multistrider exceptions
        ret_val = {
            "message": {
                "query_graph": {},
                "knowledge_graph": {
                    "nodes": {},
                    "edges": {},
                },
                "results": [],
            },
        }

    # The query_graph is getting dropped under some circumstances.  This really isn't the place to fix it
    if ("query_graph" not in ret_val["message"]) and ("message" in message):
        ret_val["message"]["query_graph"] = deepcopy(message["message"]["query_graph"])

    # make sure there is a place for the trapi log messages
    if "logs" not in ret_val:
        ret_val["logs"] = []

    # html error code returned
    if status_code != 200:
        msg = f"{name} HTML error status code {status_code} returned."

        logger.error(f"{guid}: {msg}")

        ret_val["logs"].append(create_log_entry(msg, "ERROR"))
    # good html status code
    elif len(ret_val["message"].get("results", [])) == 0:
        msg = f"{name} No results returned."

        logger.warning(f"{guid}: {msg}")

        ret_val["logs"].append(create_log_entry(msg, "WARNING"))
    else:
        logger.info(f'{guid}: {name} returned {len(ret_val["message"]["results"])} results.')

    if debug == "True":
        diff = datetime.now() - dt_start

        ret_val["logs"].append(create_log_entry(f"End of {name} processing. Time elapsed: {diff.seconds} seconds", "DEBUG"))

    return ret_val, status_code


async def lookup(message, params, guid, infer=False, caller="ARAGORN", answer_qnode=None, question_qnode=None) -> (dict, int):
    """
    Performs lookup, parameterized by ARAGORN/ROBOKOP and whether the query is an infer type query

    :param message:
    :param params:
    :param guid:
    :param caller:
    :return:
    """

    if caller == "ARAGORN":
        return await aragorn_lookup(message, params, guid, infer, answer_qnode)
    elif caller == "ROBOKOP":
        robo_results, robo_status = await robokop_lookup(message, params, guid, infer, question_qnode, answer_qnode)
        return await add_provenance(robo_results), robo_status
    return f"Illegal caller {caller}", 400

async def add_provenance(message):
    """When ROBOKOP looks things up via plater, the provenance is just from plater.  We need to go through
    each knowledge_graph edge and add an aggregated knowledge source of infores:robokop to it"""
    new_provenance = {"resource_id": "infores:robokop", "resource_role": "aggregator_knowledge_source",
                      "upstream_resource_ids": ["infores:automat-robokop"]}
    for edge in message["message"]["knowledge_graph"]["edges"].values():
        edge["sources"].append(new_provenance)
    return message

def chunk(input, n):
    for i in range(0, len(input), n):
        yield input[i : i + n]


async def aragorn_lookup(input_message, params, guid, infer, answer_qnode):
    if not infer:
        return await strider(input_message, params, guid)
    # Now it's an infer query.
    messages = expand_query(input_message, params, guid)
    lookup_query_graph = messages[0]["message"]["query_graph"]
    nrules_per_batch = int(os.environ.get("MULTISTRIDER_BATCH_SIZE", 101))
    #nrules_per_batch = int(os.environ.get("MULTISTRIDER_BATCH_SIZE", 1))
    # nrules = int(os.environ.get("MAXIMUM_MULTISTRIDER_RULES",len(messages)))
    nrules = int(os.environ.get("MAXIMUM_MULTISTRIDER_RULES", 101))
    result_messages = []
    num = 0
    num_batches_returned = 0
    for to_run in chunk(messages[:nrules], nrules_per_batch):
        message = {}
        for q in to_run:
            num += 1
            message[f"query_{num}"] = q
        logger.info(f"Sending {len(message)} messages to strider")
        batch_result_messages = await multi_strider(message, params, guid)
        num_batches_returned += 1
        logger.info(f"{guid}: {num_batches_returned} batches returned")
        for result in batch_result_messages:
            rmessage = await to_jsonable_dict(PDResponse.parse_obj(result).dict(exclude_none=True))
            await filter_repeated_nodes(rmessage, guid)
            result_messages.append(rmessage)
    logger.info(f"{guid}: strider complete")
    #Clean out the repeat node stuff
    # We have to stitch stuff together again
    #with open(f"{guid}_individual_multistrider.json", "w") as f:
    #    json.dump(result_messages, f, indent=2)
    mergedresults = await combine_messages(answer_qnode, input_message["message"]["query_graph"],
                                           lookup_query_graph, result_messages)
    #with open(f"{guid}_merged_multistrider.json", "w") as f:
    #    json.dump(mergedresults, f, indent=2)
    logger.info(f"{guid}: results merged")
    return mergedresults, 200


def merge_results_by_node_op(message, params, guid) -> (dict, int):
    qn = params["merge_qnode"]
    merged_results = merge_results_by_node(message, qn)
    return merged_results, 200


async def strider(message, params, guid) -> (dict, int):
    # strider_url = os.environ.get("STRIDER_URL", "https://strider-dev.apps.renci.org/1.3/")
    strider_url = os.environ.get("STRIDER_URL", "https://strider.renci.org/1.4/")
    #strider_url = os.environ.get("STRIDER_URL", "https://strider.transltr.io/1.3/")

    # select the type of query post. "test" will come from the tester
    if "test" in message:
        strider_url += "query"
        asyncquery = False
    else:
        strider_url += "asyncquery"
        asyncquery = True

    response = await subservice_post("strider", strider_url, message, guid, asyncquery=asyncquery)

    return response


async def normalize_qgraph_ids(m):
    url = f'{os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/1.3/")}get_normalized_nodes'
    qnodes = m["message"]["query_graph"]["nodes"]
    qnode_ids = set()
    for qid, qnode in qnodes.items():
        if ("ids" in qnode) and (qnode["ids"] is not None):
            qnode_ids.update(qnode["ids"])
    nnp = { "curies": list(qnode_ids), "conflate": True }
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=120)) as client:
        nnresult = await client.post(
            url,
            json=nnp,
        )
    if nnresult.status_code == 200:
        nnresult = nnresult.json()
        for qid, qnode in qnodes.items():
            if 'ids' in qnode and qnode['ids'] is not None:
                new_ids = [ nnresult[i]["id"]["identifier"] for i in qnode["ids"]]
                qnode["ids"] = new_ids
    else:
        logger.error("Error reaching node normalizer: {nnresult.status_code}")
    return m


async def robokop_lookup(message, params, guid, infer, question_qnode, answer_qnode) -> (dict, int):
    # For robokop, gotta normalize
    message = await normalize_qgraph_ids(message)
    if not infer:
        kg_url = os.environ.get("ROBOKOPKG_URL", "https://automat.renci.org/robokopkg/1.4/")
        return await subservice_post("robokopkg", f"{kg_url}query", message, guid)

    # It's an infer, just look it up
    rokres = await robokop_infer(message, guid, question_qnode, answer_qnode)
    return rokres
    # return await normalize(rokres,params,guid)

def get_key(predicate, qualifiers):
    keydict = {'predicate': predicate}
    keydict.update(qualifiers)
    return json.dumps(keydict,sort_keys=True)

def expand_query(input_message, params, guid):
    #Contract: 1. there is a single edge in the query graph 2. The edge is marked inferred.   3. Either the source
    #          or the target has IDs, but not both. 4. The number of ids on the query node is 1.
    for edge_id, edge in input_message["message"]["query_graph"]["edges"].items():
        source = edge["subject"]
        target = edge["object"]
        predicate = edge["predicates"][0]
        qc = edge.get("qualifier_constraints",[])
        if len(qc) == 0:
            qualifiers = {}
        else:
            qualifiers = { "qualifier_constraints": qc}
    if ("ids" in input_message["message"]["query_graph"]["nodes"][source]) \
            and (input_message["message"]["query_graph"]["nodes"][source]["ids"] is not None):
        input_id= input_message["message"]["query_graph"]["nodes"][source]["ids"][0]
        source_input = True
    else:
        input_id= input_message["message"]["query_graph"]["nodes"][target]["ids"][0]
        source_input = False
    key = get_key(predicate,qualifiers)
    #We want to run the non-inferred version of the query as well
    qg = deepcopy(input_message["message"]["query_graph"])
    for eid,edge in qg["edges"].items():
        del edge["knowledge_type"]
    messages = [{"message": {"query_graph":qg}}]
    #If we don't have any AMIE expansions, this will just generate the direct query
    for rule_def in AMIE_EXPANSIONS.get(key,[]):
        query_template = Template(json.dumps(rule_def["template"]))
        #need to do a bit of surgery depending on what the input is.
        if source_input:
            qs = query_template.substitute(source=source,target=target,source_id = input_id, target_id='')
        else:
            qs = query_template.substitute(source=source, target=target, target_id=input_id, source_id='')
        query = json.loads(qs)
        if source_input:
            del query["query_graph"]["nodes"][target]["ids"]
        else:
            del query["query_graph"]["nodes"][source]["ids"]
        message = {"message": query}
        if "log_level" in input_message:
            message["log_level"] = input_message["log_level"]
        messages.append(message)
    return messages


async def multi_strider(messages, params, guid):
    #strider_url = os.environ.get("STRIDER_URL", "https://strider-dev.apps.renci.org/1.3/")
    strider_url = os.environ.get("STRIDER_URL", "https://strider.renci.org/1.4/")

    strider_url += "multiquery"
    #We don't want to do subservice_post, because that assumes TRAPI in and out.
    #it leads to confusion.
    #response, status_code = await subservice_post("strider", strider_url, messages, guid, asyncquery=True)
    responses = await post_with_callback(strider_url,messages,guid)

    return responses


def create_aux_graph(analysis):
    """Given an analysis, create an auxiliary graph.
    Look through the analysis edge bindings, get all the knowledge edges, and put them in an aux graph.
    Give it a random uuid as an id."""
    aux_graph_id = str(uuid.uuid4())
    aux_graph = { "edges": [] }
    for edge_id, edgelist in analysis["edge_bindings"].items():
        for edge in edgelist:
            aux_graph["edges"].append(edge["id"])
    return aux_graph_id, aux_graph


def add_knowledge_edge(result_message, aux_graph_ids, answer):
    """Create a new knowledge edge in the result message, with the aux graph ids as support."""
    # Find the subject, object, and predicate of the original query
    query_graph = result_message["message"]["query_graph"]
    #get the first key and value from the edges
    qedge_id, qedge = next(iter(query_graph["edges"].items()))
    #For the nodes, if there is an id, then use it in the knowledge edge. If there is not, then use the answer
    qnode_subject_id = qedge["subject"]
    qnode_object_id = qedge["object"]
    if "ids" in query_graph["nodes"][qnode_subject_id] and query_graph["nodes"][qnode_subject_id]["ids"] is not None:
        qnode_subject = query_graph["nodes"][qnode_subject_id]["ids"][0]
        qnode_object = answer
    else:
        qnode_subject = answer
        qnode_object = query_graph["nodes"][qnode_object_id]["ids"][0]
    predicate = qedge["predicates"][0]
    # Create a new knowledge edge
    new_edge_id = str(uuid.uuid4())
    new_edge = {
        "subject": qnode_subject,
        "object": qnode_object,
        "predicate": predicate,
        "attributes": [
            {
                "attribute_type_id": "biolink:support_graphs",
                "value": aux_graph_ids
            }
        ],
        # Aragorn is the primary ks because aragorn inferred the existence of this edge.
        "sources": [{"resource_id":"infores:aragorn", "resource_role":"primary_knowledge_source"}]
    }
    result_message["message"]["knowledge_graph"]["edges"][new_edge_id] = new_edge
    return new_edge_id

def get_edgeset(result):
    """Given a result, return a frozenset of any knowledge edges in it"""
    edgeset = set()
    for analysis in result["analyses"]:
        for edge_id, edgelist in analysis["edge_bindings"].items():
            edgeset.update([e["id"] for e in edgelist])
    return frozenset(edgeset)

def merge_answer(result_message, answer, results, qnode_ids):
    """Given a set of results and the node identifiers of the original qgraph,
    create a single message.
    result_message has to contain the original query graph
    The original qgraph is a creative mode query, which has been expanded into a set of
    rules and run as straight queries using either strider or robokopkg.
    results contains both the lookup results and the creative results, separated out by keys
    Each result coming in is now structured like this:
    result
        node_bindings: Binding to the rule qnodes. includes bindings to original qnode ids
        analysis:
            edge_bindings: Binding to the rule edges.
    To merge the answer, we need to
    0) Filter out any creative results that exactly replicate a lookup result
    1) create node bindings for the original creative qnodes
    2) convert the analysis of each input result into an auxiliary graph
    3) Create a knowledge edge corresponding to the original creative query edge
    4) add the aux graphs as support for this knowledge edge
    5) create an analysis with an edge binding from the original creative query edge to the new knowledge edge
    6) add any lookup edges to the analysis directly
    """
    # 0. Filter out any creative results that exactly replicate a lookup result
    # How does this happen?   Suppose it's an inferred treats.  Lookup will find a direct treats
    # But a rule that ameliorates implies treats will also return a direct treats because treats
    # is a subprop of ameliorates. We assert that the two answers are the same if the set of their
    # kgraph edges are the same.
    # There are also cases where subpredicates in rules can lead to the same answer.  So here we
    # also unify that.   If we decide to pass rules along with the answers, we'll have to be a bit
    # more careful.
    lookup_edgesets = [get_edgeset(result) for result in results["lookup"]]
    creative_edgesets = set()
    creative_results = []
    for result in results["creative"]:
        creative_edges = get_edgeset(result)
        if creative_edges in lookup_edgesets:
            continue
        elif creative_edges in creative_edgesets:
            continue
        else:
            creative_edgesets.add(creative_edges)
            creative_results.append(result)
    results["creative"] = creative_results
    # 1. Create node bindings for the original creative qnodes and lookup qnodes
    mergedresult = {"node_bindings": {}, "analyses": []}
    serkeys = defaultdict(set)
    for q in qnode_ids:
        mergedresult["node_bindings"][q] = []
        for result in results["creative"] + results["lookup"]:
            for nb in result["node_bindings"][q]:
                serialized_binding = json.dumps(nb,sort_keys=True)
                if serialized_binding not in serkeys[q]:
                    mergedresult["node_bindings"][q].append(nb)
                    serkeys[q].add(serialized_binding)

    # 2. convert the analysis of each input result into an auxiliary graph
    aux_graph_ids = []
    if "auxiliary_graphs" not in result_message["message"] or result_message["message"]["auxiliary_graphs"] is None:
        result_message["message"]["auxiliary_graphs"] = {}
    for result in results["creative"]:
        for analysis in result["analyses"]:
            aux_graph_id, aux_graph = create_aux_graph(analysis)
            result_message["message"]["auxiliary_graphs"][aux_graph_id] = aux_graph
            aux_graph_ids.append(aux_graph_id)

    # 3. Create a knowledge edge corresponding to the original creative query edge
    # 4. and add the aux graphs as support for this knowledge edge
    knowledge_edge_ids = []
    if len(aux_graph_ids) > 0:
        #only do this if there are creative results.  There could just be a lookup
        for nid in answer:
            knowledge_edge_id = add_knowledge_edge(result_message, aux_graph_ids, nid)
            knowledge_edge_ids.append(knowledge_edge_id)

    # 5. create an analysis with an edge binding from the original creative query edge to the new knowledge edge
    qedge_id = list(result_message["message"]["query_graph"]["edges"].keys())[0]
    analysis = {
        "resource_id": "infores:aragorn",
        "edge_bindings": {qedge_id:[ { "id":kid } for kid in knowledge_edge_ids ] }
                }
    mergedresult["analyses"].append(analysis)

    # 6. add any lookup edges to the analysis directly
    for result in results["lookup"]:
        for analysis in result["analyses"]:
            for qedge in analysis["edge_bindings"]:
                if qedge not in mergedresult["analyses"][0]["edge_bindings"]:
                    mergedresult["analyses"][0]["edge_bindings"][qedge] = []
                mergedresult["analyses"][0]["edge_bindings"][qedge].extend(analysis["edge_bindings"][qedge])

    #result_message["message"]["results"].append(mergedresult)
    return mergedresult


# TODO move into operations? Make a translator op out of this
def merge_results_by_node(result_message, merge_qnode, lookup_results):
    """This assumes a single result message, with a single merged KG.  The goal is to take all results that share a
    binding for merge_qnode and combine them into a single result.
    Assumes that the results are not scored."""
    grouped_results = group_results_by_qnode(merge_qnode, result_message, lookup_results)
    original_qnodes = result_message["message"]["query_graph"]["nodes"].keys()
    # TODO : I'm sure there's a better way to handle this with asyncio
    new_results = []
    for r in grouped_results:
        new_result = merge_answer(result_message, r, grouped_results[r], original_qnodes)
        new_results.append(new_result)
    result_message["message"]["results"] = new_results
    return result_message


def group_results_by_qnode(merge_qnode, result_message, lookup_results):
    """merge_qnode is the qnode_id of the node that we want to group by
    result_message is the response message, and its results element  contains all of the creative mode results
    lookup_results is just a results element from the lookup mode query.
    """
    original_results = result_message["message"].get("results", [])
    # group results
    grouped_results = defaultdict( lambda: {"creative": [], "lookup": []})
    # Group results by the merge_qnode
    for result_set, result_key in [(original_results, "creative"), (lookup_results, "lookup")]:
        for result in result_set:
            answer = result["node_bindings"][merge_qnode]
            bound = frozenset([x["id"] for x in answer])
            grouped_results[bound][result_key].append(result)
    return grouped_results


async def make_one_request(client, automat_url, message, sem):
    async with sem:
        r = await client.post(f"{automat_url}query", json=message)
    return r

async def robokop_infer(input_message, guid, question_qnode, answer_qnode):
    automat_url = os.environ.get("ROBOKOPKG_URL", "https://automat.transltr.io/robokopkg/1.4/")
    max_conns = os.environ.get("MAX_CONNECTIONS", 5)
    nrules = int(os.environ.get("MAXIMUM_ROBOKOPKG_RULES", 101))
    messages = expand_query(input_message, {}, guid)
    lookup_query_graph = messages[0]["message"]["query_graph"]
    logger.info(f"{guid}: {len(messages)} to send to {automat_url}")
    result_messages = []
    #limits = httpx.Limits(max_keepalive_connections=None, max_connections=max_conns)
    limit = asyncio.Semaphore(max_conns)
    # async timeout in 1 hour
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=5 * 60)) as client:
        tasks = []
        for message in messages[:nrules]:
            tasks.append(asyncio.create_task( make_one_request(client, automat_url, message, limit) ))

        responses = await asyncio.gather(*tasks)

    for response in responses:
        if response.status_code == 200:
            #Validate and clean
            rmessage = PDResponse(**response.json()).dict(exclude_none=True)
            await filter_repeated_nodes(rmessage,guid)
            num_results = len(rmessage["message"].get("results",[]))
            logger.info(f"Returned {num_results} results")
            if num_results > 0 and num_results < 10000: #more than this number of results and you're into noise.
                result_messages.append(rmessage)
        else:
            logger.error(f"{guid}: {response.status_code} returned.")
    if len(result_messages) > 0:
        #with open(f"{guid}_r_individual_answers.json", 'w') as outf:
        #    json.dump(result_messages, outf, indent=2)
        # We have to stitch stuff together again
        mergedresults = await combine_messages(answer_qnode, input_message["message"]["query_graph"],
                                               lookup_query_graph, result_messages)
        #with open(f"{guid}_r_merged.json", 'w') as outf:
        #    json.dump(result_messages, outf, indent=2)
    else:
        mergedresults = {"message": {"knowledge_graph": {"nodes": {}, "edges": {}}, "results": []}}
    # The merged results will have some expanded query, we want the original query.

    return mergedresults, 200

def queries_equivalent(query1,query2):
    """Compare 2 query graphs.  The nuisance is that there is flexiblity in e.g. whether there is a qualifier constraint
    as none or it's not in there or its an empty list.  And similar for is_set and is_set is False.
    """
    q1 = query1.copy()
    q2 = query2.copy()
    for q in [q1,q2]:
        for node in q["nodes"].values():
            if "is_set" in node and node["is_set"] is False:
                del node["is_set"]
            if "constraints" in node and len(node["constraints"]) == 0:
                del node["constraints"]
        for edge in q["edges"].values():
            if "attribute_constraints" in edge and len(edge["attribute_constraints"]) == 0:
                del edge["attribute_constraints"]
            if "qualifier_constraints" in edge and len(edge["qualifier_constraints"]) == 0:
                del edge["qualifier_constraints"]
    return q1 == q2

async def combine_messages(answer_qnode, original_query_graph, lookup_query_graph, result_messages):
    pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes": {}, "edges": {}})
    for rm in result_messages:
        pydantic_kgraph.update(KnowledgeGraph.parse_obj(rm["message"]["knowledge_graph"]))
    # Construct the final result message, currently empty
    result = PDResponse(**{
        "message": {"query_graph": {"nodes": {}, "edges": {}},
                    "knowledge_graph": {"nodes": {}, "edges": {}},
                    "results": []}}).dict(exclude_none=True)
    result["message"]["query_graph"] = original_query_graph
    result["message"]["knowledge_graph"] = pydantic_kgraph.dict()
    # The result with the direct lookup needs to be handled specially.   It's the one with the lookup query graph
    lookup_results = []  # in case we don't have any
    for result_message in result_messages:
        if queries_equivalent(result_message["message"]["query_graph"],lookup_query_graph):
            lookup_results = result_message["message"]["results"]
        else:
            result["message"]["results"].extend(result_message["message"]["results"])
    mergedresults = merge_results_by_node(result, answer_qnode, lookup_results)
    return mergedresults


async def answercoalesce(message, params, guid, coalesce_type="all") -> (dict, int):
    """
    Calls answercoalesce
    :param message:
    :param params:
    :param guid:
    :param coalesce_type:
    :return:
    """
    url = f'{os.environ.get("ANSWER_COALESCE_URL", "https://answercoalesce.renci.org/1.3/coalesce/")}{coalesce_type}'
    # url = f'{os.environ.get("ANSWER_COALESCE_URL", "https://answer-coalesce.transltr.io/1.3/coalesce/")}{coalesce_type}'

    # With the current answercoalesce, we make the result list longer, and frequently much longer.  If
    # we've already got 10s of thousands of results, let's skip this step...
    if "max_input_size" in params:
        if len(message["message"].get("results", [])) > params["max_input_size"]:
            # This is already too big, don't do anything else
            return message, 200
    return await subservice_post("answer_coalesce", url, message, guid)


async def normalize(message, params, guid) -> (dict, int):
    url = f'{os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/")}response'
    return await subservice_post("nodenorm", url, message, guid)


async def omnicorp(message, params, guid) -> (dict, int):
    """
    Calls omnicorp
    :param message:
    :param params:
    :param guid:
    :return:
    """
    url = f'{os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/1.4/")}omnicorp_overlay'

    rval, omni_status =  await subservice_post("omnicorp", url, message, guid)

    # Omnicorp is not strictly necessary.  When we get something other than a 200,
    # we still want to proceed, so we return a 200 no matter what happened.
    # Note that subservice_post will have already written an error in the TRAPI logs.
    return rval,200


async def score(message, params, guid) -> (dict, int):
    """
    Calls weight correctness followed by scoring
    :param message:
    :param params:
    :param guid:
    :return:
    """
    ranker_url = os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/1.4/")

    score_url = f"{ranker_url}score"
    return await subservice_post("score", score_url, message, guid)


async def run_workflow(message, workflow, guid) -> (dict, int):
    """

    :param message:
    :param workflow:
    :param guid:
    :return:
    """
    logger.debug(f"{guid}: incoming message: {message}")

    status_code = None

    for operator_function, params, operator_id in workflow:
        #make sure results is [] rather than a key that doesn't exist or None
        if (not "results" in message["message"]) or (message["message"]["results"] is None):
            message["message"]["results"] = []

        log_message = f"Starting operation {operator_id} with {len(message['message']['results'])} results"
        add_item(guid, f"Starting operation {operator_id}", 200)

        message, status_code = await operator_function(message, params, guid)

        if status_code != 200 or "results" not in message["message"]:
            add_item(guid, f"{operator_id} failed", status_code)
            break
        elif len(message["message"]["results"]) == 0:
            add_item(guid, f"{operator_id} returned 0 results", 200)
            break

        add_item(guid, f"{operator_id} succeeded with {len(message['message']['results'])}", status_code)

        # loop through all the log entries and fix the timestamps
        if "logs" in message:
            for item in message["logs"]:
                item["timestamp"] = str(item["timestamp"])

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
        "nodes": [{"id": "a", "type": type_a, "curie": curie_a}, {"id": "b", "type": type_b}],
        "edges": [{"id": "ab", "source_id": "a", "target_id": "b"}],
    }

    if edge_type is not None:
        query_graph["edges"][0]["type"] = edge_type

        if reverse:
            query_graph["edges"][0]["source_id"] = "b"
            query_graph["edges"][0]["target_id"] = "a"

    message = {"message": {"query_graph": query_graph, "knowledge_graph": {"nodes": [], "edges": []}, "results": []}}
    return message
