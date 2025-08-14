# Text2Cypher

This project implements a multi-agent system to convert natural language questions to Cypher queries. It uses a series of agents for entity extraction, schema verification, query generation, and refinement.

## Setup

1.  Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

2.  Add your Hugging Face API token to `configs/config.json`.

3.  Set up a Neo4j database and add the connection details to `configs/config.json`.

4.  Download the dataset:
    ```bash
    python download_dataset.py
    ```

## Usage

Run the main script:
```bash
python main.py
```
