import json
import csv
import torch
from tqdm import tqdm
from agents.entity_extraction_agent import EntityExtractionAgent
from agents.schema_verification_agent import SchemaVerificationAgent
from agents.query_generation_agent import QueryGenerationAgent
from evaluation.exact_match import exact_match_score
from evaluation.bleu_score import bleu_score
from agents.refinement_agent import RefinementAgent

from utils.neo4j_connection import create_neo4j_driver

from neo4j.time import DateTime
import argparse

def _json_serialize_result(obj):
    if isinstance(obj, DateTime):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def execute_query(driver, query):
    """Executes a Cypher query and returns the result."""
    try:
        with driver.session() as session:
            result = session.run(query)
            # Convert results to a list of dictionaries, handling DateTime objects
            return json.loads(json.dumps([record.data() for record in result], default=_json_serialize_result))
    except Exception as e:
        print(f"Query execution failed: {e}")
        return {"error": str(e)}

def normalize_cypher_query(query):
    """Normalizes a Cypher query for consistent comparison."""
    # Remove leading/trailing whitespace
    query = query.strip()
    # Convert to lowercase (Cypher is case-insensitive for keywords)
    query = query.lower()
    # Remove multiple spaces and newlines
    query = ' '.join(query.split())
    return query

def main():
    # Load config
    with open("/Users/dhrumeen/projects/Text2Cypher/configs/config.json", 'r') as f:
        config = json.load(f)

    # Initialize agents (models are not loaded yet)
    parser = argparse.ArgumentParser(description="Text2Cypher Evaluation")
    parser.add_argument("--model_name", type=str, default="gemma3_4b", help="Model name to use", choices=list(config["models"].keys()))
    args = parser.parse_args()
    model_name = args.model_name
    model_id = config["models"][model_name]
    if torch.cuda.is_available():
        device = "cuda"
    elif torch.backends.mps.is_available():
        device = "mlx"
    else:
        device = "cpu"
    entity_extraction_agent = EntityExtractionAgent(config["huggingface_api_token"], model_id, device)
    schema_verification_agent = SchemaVerificationAgent("/Users/dhrumeen/projects/Text2Cypher/data/schema.json")
    query_generation_agent = QueryGenerationAgent(config["huggingface_api_token"], model_id, device)

    # Load models once
    entity_extraction_agent._load_model()
    query_generation_agent._load_model(model_id)

    # Load data
    with open("/Users/dhrumeen/projects/Text2Cypher/data/text2cypher2024/test.json", 'r') as f:
        data = json.load(f)

    # Setup CSV file
    csv_file_path = "evaluation_results.csv"
    csv_headers = [
        "question", "ground_truth_query", "generated_query", "entities",
        "exact_match_score", "bleu_score", "ground_truth_result", "generated_query_result"
    ]

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(csv_headers)

        # Filter data for valid database reference alias
        filtered_data = [item for item in data if item.get("database_reference_alias")]

        # Process 1 query
        for i, item in enumerate(tqdm(filtered_data[:5])):
            # The filtering for db_alias is now done before the loop
            # No need to check db_alias here anymore
            question = item["question"]
            ground_truth_query = item["cypher"]
            schema = item["schema"]
            db_alias = item["database_reference_alias"]

            db_username = db_alias.replace("neo4jlabs_demo_db_", "")
            db_password = db_username  # Simplified assumption

            driver = None
            ground_truth_result = 'ERROR'
            current_generated_query = 'ERROR'
            generated_query_result = 'ERROR'
            try:
                # neo4j connection driver
                driver = create_neo4j_driver(config["neo4j_uri"], db_username, db_password)
                refinement_agent = RefinementAgent(config["huggingface_api_token"], query_generation_agent, driver,  device)

                # 1. Entity Extraction
                print(f"\nProcessing question {i+1}: {question}")
                print("Step 1: Starting Entity Extraction...")
                extracted_elements = entity_extraction_agent.extract_entities(question, schema)
                print("Step 1: Entity Extraction complete.")

                # 2. Schema Verification
                print("Step 2: Starting Schema Verification...")
                verified_elements = schema_verification_agent.verify(extracted_elements)
                print("Step 2: Schema Verification complete.")

                # 3. Query Generation & Refinement Loop
                current_generated_query, generated_query_result = refinement_agent.refine_query(
                    question,
                    verified_elements,
                )

                # 5. Evaluation
                print(f"Ground Truth Query:\n{ground_truth_query}")
                
                normalized_generated_query = normalize_cypher_query(current_generated_query)
                normalized_ground_truth_query = normalize_cypher_query(ground_truth_query)

                # Execute ground truth query for comparison
                print("Executing ground truth query...")
                ground_truth_result = execute_query(driver, ground_truth_query)

                exact_match = exact_match_score(generated_query_result, ground_truth_result)
                bleu = bleu_score(normalized_generated_query, normalized_ground_truth_query)
                
                print(f"\nResults for {model_name}:")
                print(f"Generated Query: {current_generated_query}")
                print(f"Ground Truth Query: {ground_truth_query}")
                print(f"Exact Match: {exact_match}")
                print(f"BLEU Score: {bleu}")
                print("-" * 20)

                # Write to CSV
                writer.writerow([
                    question,
                    ground_truth_query,
                    current_generated_query,
                    json.dumps(extracted_elements),
                    exact_match,
                    bleu,
                    json.dumps(ground_truth_result),
                    json.dumps(generated_query_result)
                ])
            
            except Exception as e:
                print(f"An error occurred processing question {i+1}: {e}")
                writer.writerow([question, ground_truth_query, current_generated_query, str(e), 0, 0, ground_truth_result, generated_query_result])

            finally:
                if driver:
                    driver.close()

    # Unload models at the end
    entity_extraction_agent._unload_model()
    query_generation_agent._unload_model()

if __name__ == "__main__":
    main()