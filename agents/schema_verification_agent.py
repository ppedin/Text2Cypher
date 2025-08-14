import json

class SchemaVerificationAgent:
    def __init__(self, schema_path):
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)

    def verify(self, extracted_elements):
        verified_elements = {
            "entities": [],
            "relationships": [],
            "filters": extracted_elements.get("filters", [])
        }

        if not isinstance(extracted_elements, dict):
            return verified_elements

        entities_to_verify = extracted_elements.get("entities", [])
        if isinstance(entities_to_verify, list):
            for entity in entities_to_verify:
                entity_name = None
                if isinstance(entity, dict) and "name" in entity:
                    entity_name = entity["name"]
                elif isinstance(entity, dict) and "label" in entity:
                    entity_name = entity["label"]
                elif isinstance(entity, str):
                    entity_name = entity

                if entity_name and entity_name in self.schema["nodes"]:
                    verified_elements["entities"].append(entity_name)

        relationships_to_verify = extracted_elements.get("relationships", [])
        if isinstance(relationships_to_verify, list):
            for relationship in relationships_to_verify:
                relationship_name = None
                if isinstance(relationship, dict) and "name" in relationship:
                    relationship_name = relationship["name"]
                elif isinstance(relationship, dict) and "type" in relationship:
                    relationship_name = relationship["type"]
                elif isinstance(relationship, str):
                    relationship_name = relationship

                if relationship_name and relationship_name in self.schema["relationships"]:
                    verified_elements["relationships"].append(relationship_name)

        return verified_elements
