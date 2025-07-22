from datasets import load_dataset
import re
import json

import re

def template_cypher(query):

    result = []
    clause_pattern = re.compile(r'\b(MATCH|OPTIONAL MATCH|WHERE|WITH|AS|RETURN|WITH|DISTINCT|STARTS|ENDS|ORDER BY|LIMIT|SKIP|UNWIND|CALL|SET|REMOVE|MERGE|CREATE|DELETE|DETACH DELETE)\b', re.IGNORECASE)
    parts = clause_pattern.split(query)
    for i in range(1, len(parts), 2):
        clause = parts[i].upper()
        body = parts[i + 1] if i + 1 < len(parts) else ""

        if clause == "MATCH" or clause == "OPTIONAL MATCH":
            body = re.sub(r'\([^\)]*\)', '()', body)
            body = re.sub(r'\[[^\]]*\]', '[]', body)
            body = re.sub(r'\{[^\]]*\}', '{}', body)
            body = re.sub(r'[^\(\)\[\]\-\><]', '', body)
            result.append(f"{clause} {body.strip()}")
        else:
            result.append(clause)

    return ' '.join(result)

dataset = load_dataset("neo4j/text2cypher-2024v1", split="train")

dataset = dataset.map(lambda row: {
    "cypher_template": template_cypher(row["cypher"])
})

for i in range(5):
    print("Original:", dataset[i]["cypher"])
    print("Template:", dataset[i]["cypher_template"])
    print("===")

with open("text2cypher_with_templates.json", "w") as f:
    json.dump(dataset.to_list(), f, indent=2)