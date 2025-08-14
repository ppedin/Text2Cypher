import json
import torch
from mlx_lm import load, generate
from transformers import AutoModelForCausalLM, AutoTokenizer
import gc
from langchain_core.prompts import PromptTemplate

class RefinementAgent:
    def __init__(self, huggingface_api_token, query_generation_agent, driver, device):
        self.huggingface_api_token = huggingface_api_token
        self.device = device # Store the device
        self.model = query_generation_agent.model
        self.tokenizer = query_generation_agent.tokenizer
        self.query_generation_agent = query_generation_agent
        self.driver = driver

        self.prompt = PromptTemplate(
            input_variables=["question", "schema", "previous_query", "error_message"],
            template="""<start_of_turn>user
You are a Cypher query refinement agent. Your task is to analyze a previously generated Cypher query that failed, understand the error message, and refine the query based on the original question and the database schema.

### Original Question:
{question}

### Database Schema:
{schema}

### Previous (Failed) Cypher Query:
```cypher
{previous_query}
```

### Error Message:
{error_message}

### Instructions:
1.  Analyze the `Error Message` carefully to understand the cause of the failure.
2.  Review the `Previous (Failed) Cypher Query` in the context of the `Original Question` and `Database Schema`.
3.  Identify the part of the query that caused the error and determine how to fix it.
4.  Generate a **refined** Cypher query that addresses the error and correctly answers the original question.
5.  Ensure the refined query is syntactically correct and adheres to the schema.

Return only the refined Cypher query, enclosed in a ```cypher\n...\n``` block.
<end_of_turn>\n<start_of_turn>model
"""
        )

    def refine_query(self, question, verified_elements):
        max_retries = 5
        current_generated_query = ""
        generated_query_result = {"error": "Query not generated or executed"}

        for attempt in range(max_retries):
            print(f"Step 3: Starting Cypher Query Generation (Attempt {attempt + 1}/{max_retries})...")
            
            if attempt == 0:
                # Initial query generation
                current_generated_query = self.query_generation_agent.generate_query(
                    question, json.dumps(verified_elements), dialect="Cypher"
                )
            else:
                # Refine based on previous error
                prompt_text = self.prompt.format(
                    question=question,
                    schema=json.dumps(verified_elements, indent=2),
                    previous_query=current_generated_query,
                    error_message=generated_query_result["error"]
                )
                # self._load_model(self.model_id) # Model is loaded once in main.py
                if self.device == "cuda" and torch.cuda.is_available():
                    inputs = self.tokenizer(prompt_text, return_tensors="pt").to("cuda")
                    response = self.tokenizer.decode(self.model.generate(**inputs, max_new_tokens=500)[0], skip_special_tokens=True)
                else:
                    response = generate(self.model, self.tokenizer, prompt=prompt_text, max_tokens=500)
                # self._unload_model() # Model is unloaded once in main.py

                # Extract the generated Cypher query from the response
                if response.startswith("```cypher"):
                    response = response.split("```cypher\n", 1)[-1]
                if response.endswith("```"):
                    response = response.rsplit("```", 1)[0]
                current_generated_query = response.strip()

            if current_generated_query.startswith("```cypher"):
                current_generated_query = current_generated_query.split("```cypher\n", 1)[-1]
            if current_generated_query.endswith("```"):
                current_generated_query = current_generated_query.rsplit("```", 1)[0]
            current_generated_query = current_generated_query.strip()

            print(f"Generated Query (Attempt {attempt + 1}):\n{current_generated_query}")

            # Execute generated query
            try:
                with self.driver.session() as session:
                    result = session.run(current_generated_query)
                    generated_query_result = [record.data() for record in result]
            except Exception as e:
                generated_query_result = {"error": str(e)}

            if "error" not in generated_query_result:
                print(f"Query executed successfully on attempt {attempt + 1}.")
                return current_generated_query, generated_query_result
            else:
                print(f"Query execution failed on attempt {attempt + 1}: {generated_query_result['error']}")

        print("Max retries reached. Could not generate a successful query.")
        return current_generated_query, generated_query_result
