import torch
from mlx_lm import load, generate
from transformers import AutoModelForCausalLM, AutoTokenizer
from langchain_core.prompts import PromptTemplate
import json
import re
import gc

class EntityExtractionAgent:
    def __init__(self, huggingface_api_token, model_id, device):
        self.huggingface_api_token = huggingface_api_token
        self.model_id = model_id
        self.device = device # Store the device
        self.model = None
        self.tokenizer = None
        self.prompt = PromptTemplate(
            input_variables=["question", "schema"],
            template="""<start_of_turn>user
Given the following database schema and a natural language question, extract the relevant entities for a knowledge graph (node labels), relationships, and filters/attributes.

### Schema
{schema}

### Question
{question}

Return the answer in a structured JSON format with keys: entities, relationships, and filters.
Example:
Question: "Find all movies directed by Christopher Nolan after 2010"
JSON: {{"entities": ["Movie", "Director"], "relationships": ["DIRECTED"], "filters": ["Director's name = Christopher Nolan", "Movie release year > 2010"]}}

<end_of_turn>\n<start_of_turn>model
"""
        )

    def _load_model(self):
        if self.model is None:
            if self.device == "cuda" and torch.cuda.is_available():
                print(f"Loading model {self.model_id} on CUDA...")
                self.model = AutoModelForCausalLM.from_pretrained(self.model_id).to("cuda")
                self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)
            elif self.device == "mlx":
                print(f"Loading model {self.model_id} on MLX...")
                self.model, self.tokenizer = load(self.model_id, tokenizer_config={"token": self.huggingface_api_token})
            else:
                print(f"Loading model {self.model_id} on CPU (default)...")
                self.model = AutoModelForCausalLM.from_pretrained(self.model_id)
                self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)

    def _unload_model(self):
        if self.model is not None:
            if self.device == "cuda" and torch.cuda.is_available():
                self.model.to("cpu") # Move model to CPU before deleting
                del self.model
                del self.tokenizer
                torch.cuda.empty_cache()
            else:
                del self.model
                del self.tokenizer
            self.model = None
            self.tokenizer = None
            gc.collect()

    def extract_entities(self, question, schema):
        # self._load_model() # Model is loaded once in main.py
        prompt_text = self.prompt.format(question=question, schema=json.dumps(schema, indent=2))
        if self.device == "cuda" and torch.cuda.is_available():
            inputs = self.tokenizer(prompt_text, return_tensors="pt").to("cuda")
            outputs = self.model.generate(**inputs, max_new_tokens=500)
            response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        else:
            response = generate(self.model, self.tokenizer, prompt=prompt_text, max_tokens=500)
        # self._unload_model() # Model is unloaded once in main.py
        try:
            # Use regex to find the JSON object within the response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                json_text = json_match.group(0)
                parsed_json = json.loads(json_text)
                return parsed_json
            else:
                return {"entities": [], "relationships": [], "filters": []}
        except json.JSONDecodeError:
            return {"entities": [], "relationships": [], "filters": []}