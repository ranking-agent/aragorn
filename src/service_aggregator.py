"""Literature co-occurrence support."""
from itertools import combinations

import aio_pika
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
from src.util import create_log_entry
from src.operations import sort_results_score, filter_results_top_n, filter_kgraph_orphans, filter_message_top_n
from src.results_cache import ResultsCache
from src.process_db import add_item
from datetime import datetime
from requests.models import Response
from requests.exceptions import ConnectionError
from asyncio.exceptions import TimeoutError
from reasoner_pydantic import Query, KnowledgeGraph, QueryGraph
from reasoner_pydantic import Response as PDResponse
from src.shadowfax import shadowfax
import uuid

DUMPTRUCK = False

logger = logging.getLogger(__name__)

# declare the directory where the async data files will exist
queue_file_dir = "./queue-files"

#Load in the AMIE rules.
thisdir = os.path.dirname(__file__)
rulefiles = [os.path.join(thisdir,"rules","kara_typed_rules","rules_with_types_cleaned_finalized.json")]
rulefiles.append( os.path.join(thisdir, "rules", "MCQ.json"))
AMIE_EXPANSIONS = {}
for rulefile in rulefiles:
    with open(rulefile,'r') as inf:
        AMIE_EXPANSIONS.update(json.load(inf))

def examine_query(message):
    """Decides whether the input is an infer. Returns the grouping node"""
    # Currently, we support:
    # queries that are any shape with all lookup edges
    # OR
    # A 1-hop infer query.
    # OR
    # Pathfinder query
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
    pathfinder = n_infer_edges == 3
    if n_infer_edges > 1 and n_infer_edges and not pathfinder:
        raise Exception("Only a single infer edge is supported", 400)
    if (n_infer_edges > 0) and (n_infer_edges < len(qedges)):
        raise Exception("Mixed infer and lookup queries not supported", 400)
    infer = n_infer_edges == 1
    if not infer:
        return infer, None, None, pathfinder
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
    return infer, question_node, answer_node, pathfinder


def match_results_to_query(results, query_message, query_source, query_target, query_qedge_id):
    #Given a cached results and the input query, along the the query id for the source, target and edge
    # rewrite the results to match the query.

    #First, get the source, target, and qedge id's from the results
    _, _, _, results_source, _, results_target, results_qedge_id, _, _ = get_infer_parameters(results)

    #Now replace the results query graph with the input query graph
    results["message"]["query_graph"] = query_message["message"]["query_graph"]

    #Loop through the results and replace the source and target node ids with the input query node ids
    node_map = {results_source: query_source, results_target: query_target}
    for result in results["message"]["results"]:
        for result_id, query_id in node_map.items():
            result["node_bindings"][query_id] = result["node_bindings"].pop(result_id)
        for analysis in result["analyses"]:
            analysis["edge_bindings"][query_qedge_id] = analysis["edge_bindings"].pop(results_qedge_id)

    return results


async def entry(message, guid, coalesce_type, caller) -> (dict, int):
    """
    Performs a operation that calls numerous services including strider, aragorn-ranker and answer coalesce

    :param message: should be of a TRAPI Message
    :param guid:
    :param coalesce_type: what kind of answer coalesce type should be performed
    :return: the result of the request and the status code
    """

    try:
        infer, question_qnode, answer_qnode, pathfinder = examine_query(message)
    except Exception as e:
        print(e)
        return None, 500


    #we grab this stuff here so we can get it into lookup
    bypass_cache = message.get("bypass_cache", False)
    overwrite_cache = (message.get("parameters") or {}).get("overwrite_cache", False)
    overwrite_cache = overwrite_cache if type(overwrite_cache) is bool else False

    # A map from operations advertised in our x-trapi to functions
    # This is to functions rather than e.g. service urls because we may combine multiple calls into one op.
    #  e.g. our score operation will include both weighting and scoring for now.
    # Also gives us a place to handle function specific logic
    known_operations = {
        "lookup": partial(lookup, caller=caller, infer=infer, pathfinder=pathfinder, answer_qnode=answer_qnode, question_qnode=question_qnode, bypass_cache=bypass_cache),
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
        elif pathfinder: 
            workflow_def = [
                {"id": "lookup"},
                {"id": "overlay_connect_knodes"},
                {"id": "score"},
                {"id": "filter_message_top_n", "parameters": {"max_results": 500}}
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

    read_from_cache = not (bypass_cache or overwrite_cache) and not pathfinder

    try:
        query_graph = message["message"]["query_graph"]
    except KeyError:
        return f"No query graph", 422
    results_cache = ResultsCache()
    results = None
    if infer:
        # We're going to cache infer queries, and we need to do that even if we're overriding the cache
        # because we need these values to post to the cache at the end.
        input_id, predicate, qualifiers, source, source_input, target, qedge_id, mcq, member_ids = get_infer_parameters(message)
        if read_from_cache:
            results = results_cache.get_result(input_id, predicate, qualifiers, source_input, caller, workflow_def, mcq, member_ids)
            if results is not None:
                logger.info(f"{guid}: Returning results cache lookup")
                # The results can't go verbatim.  While the essense of the query is the same as the cached result,
                # the details may differ. In particular the names of the query nodes and edges may be different.
                results = match_results_to_query(results, message, source, target, qedge_id)
                return results, 200
            else:
                logger.info(f"{guid}: Results cache miss")
    else:
        mcq = False
        member_ids = []
        if read_from_cache:
            results = results_cache.get_lookup_result(workflow_def, query_graph)
            if results is not None:
                logger.info(f"{guid}: Returning results cache lookup")
                return results, 200

    workflow = []


    for op in workflow_def:
        try:
            workflow.append((known_operations[op["id"]], op.get("parameters") or {}, op["id"]))
        except KeyError:
            return f"Unknown Operation: {op}", 422

    final_answer, status_code = await run_workflow(message, workflow, guid)

    # return the workflow def so that the caller can see what we did
    final_answer["workflow"] = workflow_def

    # If we got here, we recalculated (otherwise we would have returned already).
    # so we want to write to the cache if bypass cache is false or overwrite_cache is true
    if overwrite_cache or (not bypass_cache):
        if infer:
            results_cache.set_result(input_id, predicate, qualifiers, source_input, caller, workflow_def, mcq, member_ids, final_answer)
        elif {"id": "lookup"} in workflow_def and not pathfinder:
            # We won't cache pathfinder results for now
            results_cache.set_lookup_result(workflow_def, query_graph, final_answer)

    # return the answer
    return final_answer, status_code


def is_end_message(message):
    if message.get("status_communication", {}).get("strider_multiquery_status", "running") == "complete":
        return True
    return False


async def post_with_callback(host_url, query, guid, params={}, bypass_cache=False):
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
        query["bypass_cache"] = bypass_cache
    else:
        for qname, individual_query in query.items():
            individual_query["callback"] = callback_url
            individual_query["bypass_cache"] = bypass_cache
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
            post_response = await client.post(
                host_url,
                json=query,
            )
        # check the response status.
        if post_response.status_code != 200:
            # queue isn't needed for failed service call
            logger.warning(f"{guid} POST status: {post_response.status_code}. Deleting unneeded queue.")
            await delete_queue(guid)
            # if there is an error this will return a <requests.models.Response> type
            return post_response
    except httpx.RequestError as e:
        logger.error(f"Failed to contact {host_url}")
        await delete_queue(guid)
        # exception handled in subservice_post
        raise e

    # async wait for callbacks to come on queue
    responses = await collect_callback_responses(guid, num_queries, params)
    return responses


async def collect_callback_responses(guid, num_queries, params={}):
    """Collect all callback responses.  No parsing"""
    # create the response object
    # response = Response()

    # pydantic_message = Message()
    #pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes": {}, "edges": {}})
    #accumulated_results = []

    # Don't spend any more time than this assembling messages
    #OVERALL_TIMEOUT = timedelta(hours=1)  # 1 hour
    logger.info(f"{guid}: params: {params}")
    OVERALL_TIMEOUT = timedelta(seconds=params.get("timeout_seconds") or 3 * 60)

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

async def get_pika_connection():
    q_username = os.environ.get("QUEUE_USER", "guest")
    q_password = os.environ.get("QUEUE_PW", "guest")
    q_host = os.environ.get("QUEUE_HOST", "127.0.0.1")
    connection = await aio_pika.connect_robust(host=q_host, login=q_username, password=q_password)
    return connection


async def create_queue(guid):
    connection = None
    try:
        connection = await get_pika_connection()
        async with connection:
            channel = await connection.channel()
            # declare the queue using the guid as the key
            queue = await channel.declare_queue(guid)
    except Exception as e:
        logger.error(f"{guid}: Failed to create queue.")
        raise e
    finally:
        if connection:
            await connection.close()


async def delete_queue(guid):
    connection = None
    try:
        connection = await get_pika_connection()
        async with connection:
            channel = await connection.channel()
            # delete the queue using the guid as the key
            queue = await channel.queue_delete(guid)
    except Exception:
        logger.error(f"{guid}: Failed to delete queue.")
        # Deleting queue isn't essential, so we will continue
    finally:
        if connection:
            await connection.close()


def has_unique_nodes(result):
    """Given a result, return True if all nodes are unique, False otherwise"""
    seen = set()
    for qnode, knodes in result["node_bindings"].items():
        knode_ids = frozenset([knode["id"] for knode in knodes])
        if knode_ids in seen:
            return False
        seen.add(knode_ids)
    return True

async def filter_promiscuous_results(response,guid):
    """We have some rules like A<-treats-B-part_of->C<-part_of-D.   This is saying B treats A, and D is like
    B (because they are both part of C).  This isn't the worst rule in the world, we find it statistically
    useful.  But, there are Cs that contain lllllooooootttttssss of stuff, and it creates a lot of bad results.
    Not only are they bad, but they are basically the same in terms of score, so we create a lot of ties.
    We are taking some approaches to fixing this in ranking, but really the results are just terrible, let's
    get rid of them, but distinguish cases where the rule is doing something interesting from when it is not.
    And note that "part_of" is not the only rule that follows this similarity-style pattern.   The difference is
    basically how many times C occurs.
    What we'd really like to do is not use promiscuous nodes in the C spot (or other places really).  But we
    don't have a promiscuity score for the nodes, and can't really get one. """
    #First, we need to know if we have too many results, and if it's the right kind of query
    MAX_C = 10
    if len(response["message"]["results"]) < MAX_C:
        return
    prom_qnodes = await get_promiscuous_qnodes(response)
    #This is a dictionary from bound knodes to the index of their result
    #There should only be one such node
    for qnode in prom_qnodes:
        # It's possible that there are multiple knodes that could be filtered.  But when we filter out the first one
        # then the indices of the rest will change.  So we need to do this one at a time.
        await remove_promiscuous_knode_results(MAX_C, qnode, response)


async def remove_promiscuous_knode_results(MAX_C, qnode, response):
    """Given a response and a qnode, look at all the results and count how many of the results have the
    same knode bound to that qnode.   If that number is greater than MAX_C, remove those results."""
    still_going = True
    #This is written as a loop with the idea that once we've removed one promiscuous node, it might require
    # recalculating everything since the results change.  In retrospect, that might not be true because we are
    # specifiying the qnode.  I'm still think it's possible (but perhaps unlikely) if there are multiple knodes
    # bound to the same qnode.
    while still_going:
        still_going = False
        # How many distinct results have the same bozo in this spot?
        prom_counter = defaultdict(list)
        for result_i, result in enumerate(response["message"]["results"]):
            for binding in result["node_bindings"][qnode]:
                knode = binding["id"]
                prom_counter[knode].append(result_i)
        # now figure out the most common knode
        max_knode = None
        max_count = 0
        for knode, mapped_result_indices in prom_counter.items():
            if len(mapped_result_indices) > max_count:
                max_knode = knode
                max_count = len(mapped_result_indices)
        # Now remove all the results with that knode (if it occurs in more than MAX_C results)
        if max_count > MAX_C:
            still_going = True
            #These are the indices of the results that we want to remove
            mapped_result_indices = prom_counter[max_knode]
            #Remove them from right to left, otherwise the indices change on you
            for index in reversed(mapped_result_indices):
                del response["message"]["results"][index]


async def get_promiscuous_qnodes(response):
    """We have some rules like A<-treats-B-part_of->C<-part_of-D.  Figure out if this qgraph is like that and return
    C if it is"""
    qgraph = response["message"]["query_graph"]
    if len(qgraph["edges"]) < 3:
        return []
    #for this to be a problem, we need 2 edges that share a subject or an object, and have the same predicates and qualifiers.
    subjects = defaultdict(list)
    objects = defaultdict(list)
    for qedge_id, qedge in qgraph["edges"].items():
        subjects[qedge["subject"]].append(qedge_id)
        objects[qedge["object"]].append(qedge_id)
    center_nodes=[]
    for nodelist in (subjects,objects):
        for node, edges in nodelist.items():
            if len(edges) < 2:
                continue
            for eid1,eid2 in combinations(edges, 2):
                e1 = qgraph["edges"][eid1]
                e2 = qgraph["edges"][eid2]
                if e1["predicates"] == e2["predicates"]:
                    if e1.get("qualifiers",[]) == e2.get("qualifiers",[]):
                        center_nodes.append(node)
    return center_nodes


async def filter_repeated_nodes(response,guid):
    """We have some rules that include e.g. 2 chemicals.   We don't want responses in which those two
    are the same.   If you have A-B-A-C then what shows up in the ui is B-A-C which makes no sense."""
    original_result_count = len(response["message"].get("results",[]))
    if original_result_count == 0:
        return
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
    connection = None
    try:
        connection = await get_pika_connection()
        async with connection:
            channel = await connection.channel()
            queue = await channel.get_queue(guid, ensure=True)
            # wait for the response.  Timeout after
            async with queue.iterator(timeout=CONNECTION_TIMEOUT) as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        num_responses += 1
                        logger.info(f"{guid}: Strider returned {num_responses} out of {num_queries}.")
                        jr = process_message(message)
                        if DUMPTRUCK:
                            with open(f"{guid}_{num_responses}.json","w") as outf:
                                json.dump(jr,outf,indent=2)
                        if is_end_message(jr):
                            logger.info(f"{guid}: Received complete message from multistrider")
                            complete = True
                            break

                        await de_noneify(jr)
                        if "query_graph" not in jr["message"]:
                            logger.warning(f"{guid}: No query graph in message")
                        else:
                            logger.info(f"{guid}: {len(jr.get('message',{}).get('results',[]))} results from {jr['message']['query_graph']}")
                            logger.info(f"{guid}: {len(jr.get('message',{}).get('auxiliary_graphs',[]))} auxgraphs")
                            responses.append(jr)

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
    finally:
        if connection:
            await connection.close()

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

async def subservice_post(name, url, message, guid, asyncquery=False, params={}) -> (dict, int):
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
                    result = response.json()
                    await de_noneify(result)
                    ret_val = await to_jsonable_dict(result)
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


async def lookup(message, params, guid, infer=False, pathfinder=False, caller="ARAGORN", answer_qnode=None, question_qnode=None, bypass_cache=False) -> (dict, int):
    """
    Performs lookup, parameterized by ARAGORN/ROBOKOP and whether the query is an infer type query

    :param message:
    :param params:
    :param guid:
    :param caller:
    :return:
    """
    message = await normalize_qgraph_ids(message)
    if caller == "ARAGORN":
        return await aragorn_lookup(message, params, guid, infer, pathfinder, answer_qnode, bypass_cache)
    elif caller == "ROBOKOP":
        robo_results, robo_status = await robokop_lookup(message, params, guid, infer, question_qnode, answer_qnode)
        return await add_provenance(robo_results), robo_status
    return f"Illegal caller {caller}", 400

async def add_provenance(message):
    """When ROBOKOP looks things up via plater, the provenance is just from plater.  We need to go through
    each knowledge_graph edge and add an aggregated knowledge source of infores:robokop to it"""
    new_provenance = {"resource_id": "infores:robokop", "resource_role": "aggregator_knowledge_source",
                      "upstream_resource_ids": ["infores:automat-robokop"]}
    for edge in message["message"].get("knowledge_graph",{}).get("edges",{}).values():
        edge["sources"].append(new_provenance)
    return message

def chunk(input, n):
    for i in range(0, len(input), n):
        yield input[i : i + n]

async def de_noneify(message):
    """Remove all the None values from a message"""
    if isinstance(message, dict):
        keys_to_remove = []
        for key, value in message.items():
            if value is None:
                keys_to_remove.append(key)
            else:
                await de_noneify(value)
        for key in keys_to_remove:
            del message[key]
    elif isinstance(message, list):
        for item in message:
            await de_noneify(item)


async def aragorn_lookup(input_message, params, guid, infer, pathfinder, answer_qnode, bypass_cache):
    timeout_seconds = (input_message.get("parameters") or {}).get("timeout_seconds")
    if timeout_seconds:
        params["timeout_seconds"] = timeout_seconds if type(timeout_seconds) is int else 3 * 60
    if pathfinder:
        return await shadowfax(input_message, guid, logger)
    if not infer:
        return await strider(input_message, params, guid, bypass_cache)
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
        batch_result_messages = await multi_strider(message, params, guid, bypass_cache)
        num_batches_returned += 1
        logger.info(f"{guid}: {num_batches_returned} batches returned")
        for result in batch_result_messages:
            #this clean is dog slow with big messages
            #rmessage = await to_jsonable_dict(PDResponse.parse_obj(result).dict(exclude_none=True))
            await de_noneify(result)
            rmessage = await to_jsonable_dict(result)
            if "knowledge_graph" not in rmessage["message"] or "results" not in rmessage["message"]:
                continue
            await filter_repeated_nodes(rmessage, guid)
            await filter_promiscuous_results(rmessage, guid)
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
    merged_results = merge_results_by_node(message, qn, False)
    return merged_results, 200


async def strider(message, params, guid, bypass_cache) -> (dict, int):
    strider_url = os.environ.get("STRIDER_URL", "https://strider.renci.org/")

    # select the type of query post. "test" will come from the tester
    if "test" in message:
        strider_url += "query"
        asyncquery = False
    else:
        strider_url += "asyncquery"
        asyncquery = True

    message["bypass_cache"] = bypass_cache
    response = await subservice_post("strider", strider_url, message, guid, asyncquery=asyncquery, params=params)

    return response


async def normalize_qgraph_ids(m):
    url = f'{os.environ.get("NODENORM_URL", "https://nodenormalization-sri.renci.org/")}get_normalized_nodes'
    qnodes = m["message"]["query_graph"]["nodes"]
    qnode_ids = set()
    for qid, qnode in qnodes.items():
        if ("ids" in qnode) and (qnode["ids"] is not None):
            qnode_ids.update(qnode["ids"])
    nnp = { "curies": list(qnode_ids), "conflate": True, "drug_chemical_conflate": True }
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout=120)) as client:
        nnresult = await client.post(
            url,
            json=nnp,
        )
    if nnresult.status_code == 200:
        nnresult = nnresult.json()
        for qid, qnode in qnodes.items():
            if 'ids' in qnode and qnode['ids'] is not None:
                normalized_ids = [nnresult[i]["id"]["identifier"] if nnresult[i] else i for i in qnode["ids"]]
                qnode["ids"] = normalized_ids
    else:
        logger.error(f"Error reaching node normalizer: {nnresult.status_code}")
    return m


async def robokop_lookup(message, params, guid, infer, question_qnode, answer_qnode) -> (dict, int):
    if not infer:
        kg_url = os.environ.get("ROBOKOPKG_URL", "https://automat.renci.org/robokopkg/")
        return await subservice_post("robokopkg", f"{kg_url}query", message, guid)

    # It's an infer, just look it up
    rokres = await robokop_infer(message, guid, question_qnode, answer_qnode)
    return rokres

def get_infer_parameters(input_message):
    """Given an infer input message, return the parameters needed to run the infer.
    input_id: the curie of the input node
    predicate: the predicate of the inferred edge
    qualifiers: the qualifiers of the inferred edge
    source: the query node id of the source node
    target: the query node id of the target node
    source_input: True if the source node is the input node, False if the target node is the input node"""
    for edge_id, edge in input_message["message"]["query_graph"]["edges"].items():
        source = edge["subject"]
        target = edge["object"]
        query_edge = edge_id
        predicate = edge["predicates"][0]
        qc = edge.get("qualifier_constraints", [])
        if len(qc) == 0:
            qualifiers = {}
        else:
            qualifiers = {"qualifier_constraints": qc}
    mcq = False
    snode = input_message["message"]["query_graph"]["nodes"][source]
    tnode = input_message["message"]["query_graph"]["nodes"][target]
    if ("ids" in snode) and (snode["ids"] is not None):
        input_id = snode["ids"][0]
        member_ids = snode.get("member_ids",[])
        if "set_interpretation" in snode and snode["set_interpretation"] == "MANY":
            mcq = True
        source_input = True
    else:
        input_id = tnode["ids"][0]
        member_ids = tnode.get("member_ids",[])
        if "set_interpretation" in tnode and tnode["set_interpretation"] == "MANY":
            mcq = True
        source_input = False
    #key = get_key(predicate, qualifiers)
    return input_id, predicate, qualifiers, source, source_input, target, query_edge, mcq, member_ids

def get_rule_key(predicate, qualifiers, mcq):
    keydict = {'predicate': predicate}
    keydict.update(qualifiers)
    if mcq:
        keydict["mcq"] = True
    return json.dumps(keydict,sort_keys=True)

def expand_query(input_message, params, guid):
    #Contract: 1. there is a single edge in the query graph 2. The edge is marked inferred.   3. Either the source
    #          or the target has IDs, but not both. 4. The number of ids on the query node is 1.
    input_id, predicate, qualifiers, source, source_input, target, qedge_id, mcq, member_ids = get_infer_parameters(input_message)
    key = get_rule_key(predicate, qualifiers, mcq)
    #We want to run the non-inferred version of the query as well
    qg = deepcopy(input_message["message"]["query_graph"])
    for eid,edge in qg["edges"].items():
        del edge["knowledge_type"]
    messages = [{"message": {"query_graph":qg}, "parameters": input_message.get("parameters") or {}}]
    #If it's an MCQ, then we also copy the KG which has the member_of edges
    if mcq:
        messages[0]["message"]["knowledge_graph"] = deepcopy(input_message["message"]["knowledge_graph"])
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
            query["query_graph"]["nodes"][target].pop("member_ids", None)
            if mcq:
                query["query_graph"]["nodes"][source]["member_ids"] = member_ids
        else:
            del query["query_graph"]["nodes"][source]["ids"]
            query["query_graph"]["nodes"][source].pop("member_ids", None)
            if mcq:
                query["query_graph"]["nodes"][target]["member_ids"] = member_ids
        message = {"message": query, "parameters": input_message.get("parameters") or {}}
        if mcq:
            message["message"]["knowledge_graph"] = deepcopy(input_message["message"]["knowledge_graph"])
        if "log_level" in input_message:
            message["log_level"] = input_message["log_level"]
        messages.append(message)
    return messages





async def multi_strider(messages, params, guid, bypass_cache):
    strider_url = os.environ.get("STRIDER_URL", "https://strider.renci.org/")

    strider_url += "multiquery"
    #We don't want to do subservice_post, because that assumes TRAPI in and out.
    #it leads to confusion.
    #response, status_code = await subservice_post("strider", strider_url, messages, guid, asyncquery=True)
    responses = await post_with_callback(strider_url,messages,guid,params,bypass_cache)

    return responses


def create_aux_graph(analysis):
    """Given an analysis, create an auxiliary graph.
    Look through the analysis edge bindings, get all the knowledge edges, and put them in an aux graph.
    Give it a random uuid as an id."""
    aux_graph_id = str(uuid.uuid4())
    aux_graph = { "edges": [] , "attributes": []}
    for edge_id, edgelist in analysis["edge_bindings"].items():
        for edge in edgelist:
            aux_graph["edges"].append(edge["id"])
    return aux_graph_id, aux_graph


def add_knowledge_edge(result_message, aux_graph_ids, answer, robokop):
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
    if "qualifier_constraints" in qedge and qedge["qualifier_constraints"] is not None and len(qedge["qualifier_constraints"]) > 0:
        qualifiers = qedge["qualifier_constraints"][0]["qualifier_set"]
    else:
        qualifiers = None
    # Create a new knowledge edge
    new_edge_id = str(uuid.uuid4())
    if robokop:
        source = "infores:robokop"
    else:
        source = "infores:aragorn"
    new_edge = {
        "subject": qnode_subject,
        "object": qnode_object,
        "predicate": predicate,
        "attributes": [
            {
                "attribute_type_id": "biolink:support_graphs",
                "value": aux_graph_ids
            },
            {
                "attribute_type_id": "biolink:agent_type",
                "value": "computational_model",
                "attribute_source": source
            },
            {
                "attribute_type_id": "biolink:knowledge_level",
                "value": "prediction",
                "attribute_source": source
            }
        ],
        # Aragorn is the primary ks because aragorn inferred the existence of this edge.
        "sources": [{"resource_id":source, "resource_role":"primary_knowledge_source"}]
    }
    if qualifiers is not None:
        new_edge["qualifiers"] = qualifiers
    result_message["message"]["knowledge_graph"]["edges"][new_edge_id] = new_edge
    return new_edge_id

def get_edgeset(result):
    """Given a result, return a frozenset of any knowledge edges in it"""
    edgeset = set()
    for analysis in result["analyses"]:
        for edge_id, edgelist in analysis["edge_bindings"].items():
            edgeset.update([e["id"] for e in edgelist])
    return frozenset(edgeset)

def merge_answer(result_message, answer, results, qnode_ids, robokop=False):
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
            knowledge_edge_id = add_knowledge_edge(result_message, aux_graph_ids, nid, robokop)
            knowledge_edge_ids.append(knowledge_edge_id)

    # 5. create an analysis with an edge binding from the original creative query edge to the new knowledge edge
    qedge_id = list(result_message["message"]["query_graph"]["edges"].keys())[0]
    if robokop:
        source = "infores:robokop"
    else:
        source = "infores:aragorn"
    analysis = {
        "resource_id": source,
        "edge_bindings": {qedge_id:[ { "id":kid, "attributes": [] } for kid in knowledge_edge_ids ] }
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
def merge_results_by_node(result_message, merge_qnode, lookup_results, robokop=False):
    """This assumes a single result message, with a single merged KG.  The goal is to take all results that share a
    binding for merge_qnode and combine them into a single result.
    Assumes that the results are not scored."""
    grouped_results = group_results_by_qnode(merge_qnode, result_message, lookup_results)
    original_qnodes = result_message["message"]["query_graph"]["nodes"].keys()
    # TODO : I'm sure there's a better way to handle this with asyncio
    new_results = []
    for r in grouped_results:
        new_result = merge_answer(result_message, r, grouped_results[r], original_qnodes, robokop)
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
    automat_url = os.environ.get("ROBOKOPKG_URL", "https://automat.transltr.io/robokopkg/")
    max_conns = os.environ.get("MAX_CONNECTIONS", 5)
    nrules = int(os.environ.get("MAXIMUM_ROBOKOPKG_RULES", 101))
    messages = expand_query(input_message, {}, guid)
    #with open('robokop_infer.txt', 'w') as logfile:
    #    json.dump(input_message, logfile, indent=2)
    #    logfile.write("------\n")
    #    json.dump(messages, logfile, indent=2)
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

    nr = 0
    for response in responses:
        if response.status_code == 200:
            #Validate and clean
            rmessage = PDResponse(**response.json()).dict(exclude_none=True)
            await filter_repeated_nodes(rmessage,guid)
            num_results = len(rmessage["message"].get("results",[]))
            logger.info(f"Returned {num_results} results")
            if num_results > 0 and num_results < 10000: #more than this number of results and you're into noise.
                #with (open(f"{guid}_r_{nr}.json", 'w')) as outf:
                #    json.dump(rmessage, outf, indent=2)
                #    nr += 1
                result_messages.append(rmessage)
        else:
            logger.error(f"{guid}: {response.status_code} returned.")
    if len(result_messages) > 0:
        # We have to stitch stuff together again
        mergedresults = await combine_messages(answer_qnode, input_message["message"]["query_graph"],
                                               lookup_query_graph, result_messages, robokop=True)
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

async def combine_messages(answer_qnode, original_query_graph, lookup_query_graph, result_messages, robokop=False):
    pydantic_kgraph = KnowledgeGraph.parse_obj({"nodes": {}, "edges": {}})
    for rm in result_messages:
        pydantic_kgraph.update(KnowledgeGraph.parse_obj(rm["message"]["knowledge_graph"]))
    # Construct the final result message, currently empty
    result = PDResponse(**{
        "message": {
            "query_graph": {"nodes": {}, "edges": {}},
            "knowledge_graph": {"nodes": {}, "edges": {}},
            "results": [],
            "auxiliary_graphs": {},
        },
        "logs": [] }).dict(exclude_none=True)
    result["message"]["query_graph"] = original_query_graph
    result["message"]["knowledge_graph"] = pydantic_kgraph.dict()
    for rm in result_messages:
        if "auxiliary_graphs" in result["message"]:
            result["message"]["auxiliary_graphs"].update(rm["message"].get("auxiliary_graphs", {}))
    # The result with the direct lookup needs to be handled specially.   It's the one with the lookup query graph
    lookup_results = []  # in case we don't have any
    for result_message in result_messages:
        if queries_equivalent(result_message["message"]["query_graph"],lookup_query_graph):
            lookup_results = result_message["message"]["results"]
        else:
            result["message"]["results"].extend(result_message["message"]["results"])
    mergedresults = merge_results_by_node(result, answer_qnode, lookup_results, robokop)
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
    url = f'{os.environ.get("ANSWER_COALESCE_URL", "https://answercoalesce.renci.org/coalesce/")}{coalesce_type}'

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

    if DUMPTRUCK:
        with open("to_omni.json","w") as outf:
            json.dump(message, outf, indent=2)

    url = f'{os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/")}omnicorp_overlay'

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

    if DUMPTRUCK:
        with open("to_score.json","w") as outf:
            json.dump(message, outf, indent=2)

    ranker_url = os.environ.get("RANKER_URL", "https://aragorn-ranker.renci.org/")

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
