from datasets import load_dataset
import json

# Load the dataset
dataset = load_dataset("neo4j/Text2Cypher")

# Save the train and test splits
for split in ["train", "test"]:
    with open(f"/Users/dhrumeen/projects/Text2Cypher/data/text2cypher2024/{split}.json", "w") as f:
        json.dump(dataset[split].to_list(), f, indent=4)
