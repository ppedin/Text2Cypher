import torch
from mlx_lm import load, generate
from transformers import AutoModelForCausalLM, AutoTokenizer
from langchain_core.prompts import PromptTemplate
import gc
import json

class QueryGenerationAgent:
    def __init__(self, huggingface_api_token, model_id, device):
        self.huggingface_api_token = huggingface_api_token
        self.model_id = model_id
        self.device = device # Store the device
        self.tokenizer = None
        self.model = None
        self.loaded_model_id = None

        self.prompt = PromptTemplate(
            input_variables=["question", "entities", "relationships", "filters", "dialect"],
            template="""<start_of_turn>user
Your task is to generate a Cypher query using **only** the verified entities, relationships, and filters provided below. Do not use any graph elements not listed.

### Verified Graph Elements
Entities: {entities}
Relationships: {relationships}
Filters: {filters}

### Question
{question}

### Instructions
1. Use only the verified entities, relationships, and filters.
2. Map relevant concepts from the question to those in the verified lists.
3. Build the Cypher query step-by-step:
    - Use `MATCH` for graph patterns; `OPTIONAL MATCH` when appropriate.
    - Apply all relevant `WHERE` filters.
    - Use aggregations (`count`, `avg`, `sum`, `max`, etc.) if required.
    - Use `RETURN` to specify the required output, including specific properties if asked.
    - Apply `ORDER BY`, `LIMIT`, or `SKIP` if relevant.
4. Do not invent or use any elements, properties, or logic outside the verified elements.
5. Format the final query for clarity.

---

### Example 1: Aggregation & Filtering

**Question:**  
What is the average price of products supplied by vendors from Germany?

**Verified Graph Elements:**  
Entities: Product, Vendor  
Relationships: SUPPLIED_BY  
Filters: Vendor.country = 'Germany'  
Properties: Product.price

**Reasoning:**  
- Use only Product, Vendor, SUPPLIED_BY.
- Filter vendors where country is Germany.
- Compute average price of such products.

**Cypher Query ({dialect}):**
```cypher
MATCH (p:Product)-[:SUPPLIED_BY]->(v:Vendor)
WHERE v.country = 'Germany'
RETURN avg(p.price) AS average_price
```

### Example 2: Sorting, Limiting, Multiple Filters

**Question:**
List the top 5 employees hired after 2020 who have a salary above $100,000, showing their names and salaries.

**Verified Graph Elements:**
Entities: Employee
Relationships: (none)
Filters: Employee.hire_date > '2020-01-01', Employee.salary > 100000
Properties: Employee.name, Employee.salary

**Reasoning:**
- Use only Employee.
- Filter by hire date and salary.
- Return name and salary.
- Sort by salary descending, limit to 5.

**Cypher Query ({dialect}):**
```cypher
MATCH (e:Employee)
WHERE e.hire_date > '2020-01-01' AND e.salary > 100000
RETURN e.name, e.salary
ORDER BY e.salary DESC
LIMIT 5
```

### Example 3: Aggregation, Grouping, and Optional Data

**Question:**
For each author, how many books have they published? Include authors even if they have published zero books.

**Verified Graph Elements:**
Entities: Author, Book
Relationships: WROTE
Filters: (none)
Properties: Author.name

**Reasoning:**
- Use only Author, Book, WROTE.
- Use OPTIONAL MATCH to include authors without books.
- Count number of books for each author.

**Cypher Query ({dialect}):**
```cypher
MATCH (a:Author)
OPTIONAL MATCH (a)-[:WROTE]->(b:Book)
RETURN a.name, count(b) AS books_published
```
---

**Your Turn**

**Verified Graph Elements:**
Entities: {entities}
Relationships: {relationships}
Filters: {filters}

**Question:**
{question}

**Cypher Query ({dialect}):}}
<end_of_turn>
<start_of_turn>model
"""
        )

    def _load_model(self, model_id):
        if self.loaded_model_id == model_id:
            return

        if self.model is not None:
            self._unload_model()

        if self.device == "cuda" and torch.cuda.is_available():
            print(f"Loading model {self.model_id} on CUDA...")
            self.model = AutoModelForCausalLM.from_pretrained(self.model_id).to("cuda")
            self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)
        elif self.device == "mlx":
            print(f"Loading model {self.model_id} on MLX...")
            self.model, self.tokenizer = load(self.model_id, tokenizer_config={"token": self.huggingface_api_token})
        else:
            print(f"Loading model {self.model_id} on CPU (default)...")
            # Default to CPU if no specific device or CUDA not available
            self.model, self.tokenizer = load(self.model_id, tokenizer_config={"token": self.huggingface_api_token})

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

    def generate_query(self, question, schema, dialect="Cypher"):
        # self._load_model(model_id) # Model is loaded once in main.py
        parsed_schema = json.loads(schema)
        entities = parsed_schema.get("entities", [])
        relationships = parsed_schema.get("relationships", [])
        filters = parsed_schema.get("filters", [])
        prompt_text = self.prompt.format(question=question, entities=entities, relationships=relationships, filters=filters, dialect=dialect)
        if self.device == "cuda" and torch.cuda.is_available():
            inputs = self.tokenizer(prompt_text, return_tensors="pt").to("cuda")
            outputs = self.model.generate(**inputs, max_new_tokens=500)
            response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        else:
            response = generate(self.model, self.tokenizer, prompt=prompt_text, max_tokens=500)
        # self._unload_model() # Model is unloaded once in main.py
        return response
