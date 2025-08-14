import neo4j
from neo4j import Query, GraphDatabase
from .execution_accuracy import _compare_execution, to_hashable
from langchain_community.graphs import Neo4jGraph



def exact_match(pred_cypher: str,
               target_cypher: str,
               neo4j_connector,
               timeout: int = 10) -> float:
    """Whether the predicted Cypher query is executable"""
    if type(pred_cypher) != str:
        return 0.0
    if pred_cypher == target_cypher:
        return 1.0
    if '<think>' in pred_cypher:
        return 0.0
    try:
        generated_result = neo4j_connector.run_query(pred_cypher, timeout=timeout, convert_to_hashable=True)
        generated_result = [{k: to_hashable(v) for k, v in record.items()} for record in generated_result]

        # with GraphDatabase.driver(neo4j_connector.URI, 
        #                                 auth=(neo4j_connector.username, neo4j_connector.username),
        #                                 connection_timeout=120,
        #                                 notifications_min_severity='OFF',  # or 'WARNING' to enable entirely
        #                                 # notifications_disabled_classifications=['HINT', 'GENERIC'],
        #                                 ) as con:
        #     con.verify_connectivity()
        # generated_result = con.execute_query(
        #     Query(pred_cypher, timeout=timeout),
        #     database_=neo4j_connector.username,
        #     )
        # generated_result = convert_dict_to_str(generated_result)
    except (
            neo4j.exceptions.CypherSyntaxError,
            neo4j.exceptions.DatabaseError,
            neo4j.exceptions.CypherTypeError,
            neo4j.exceptions.ClientError,
    ) as e:
        # print(f"Warning: Exception {e} occurred while executing the predicted Cypher query {pred_cypher}")
        return 0.0
    except TypeError as e:
        # TODO: For some queries (e.g. queries that bind the path to a variable), the result is not hashable
        # However, currently we don't have such queries in the benchmark
        # So this exception indicates the predicted Cypher query is incorrect
        return 0.0
    except Exception as e:
        # print(f"Warning: Exception {e} occurred while executing the predicted Cypher query {pred_cypher}")
        return 0.0
    # con.close()
    
    try: 
        target_result = neo4j_connector.run_query(target_cypher, timeout=timeout, convert_to_hashable=True)
        target_result = [{k: to_hashable(v) for k, v in record.items()} for record in target_result]
    except TypeError as e:
        # some items in target cypher output is not hashable
        return 0.0
    except Exception as e:
        return 0.0
    try:
        output = compare_execution(generated_result, target_result, order_matters=False)
    except Exception as e:
        return 0.0
    return output

# ocrrect exact_match, 
# def exact_match(pred_cypher: str,
#                target_cypher: str,
#                neo4j_connector: Neo4jConnector,
#                timeout: int = 60) -> float:
#     """Whether the predicted Cypher query is executable"""
#     if pred_cypher == target_cypher:
#         return 1.0
#     try:
#         generated_result = neo4j_connector.run_query(pred_cypher, timeout=timeout, convert_to_str=True)


#         # with GraphDatabase.driver(neo4j_connector.URI, 
#         #                                 auth=(neo4j_connector.username, neo4j_connector.username),
#         #                                 connection_timeout=120,
#         #                                 notifications_min_severity='OFF',  # or 'WARNING' to enable entirely
#         #                                 # notifications_disabled_classifications=['HINT', 'GENERIC'],
#         #                                 ) as con:
#         #     con.verify_connectivity()
#         # generated_result = con.execute_query(
#         #     Query(pred_cypher, timeout=timeout),
#         #     database_=neo4j_connector.username,
#         #     )
#         # generated_result = convert_dict_to_str(generated_result)
#     except (
#             neo4j.exceptions.CypherSyntaxError,
#             neo4j.exceptions.DatabaseError,
#             neo4j.exceptions.CypherTypeError,
#             neo4j.exceptions.ClientError,
#     ) as e:
#         # print(f"Warning: Exception {e} occurred while executing the predicted Cypher query {pred_cypher}")
#         return e
#     except Exception as e:
#         # print(f"Warning: Exception {e} occurred while executing the predicted Cypher query {pred_cypher}")
#         return e
#     # con.close()
#     target_result = neo4j_connector.run_query(target_cypher, timeout=timeout, convert_to_str=True)
#     exact_match = evaluate.load("exact_match")
#     try:
#         output = exact_match.compute(predictions=[generated_result], references=[target_result])
#     except Exception as e:
#         return e
#     return output['exact_match']
