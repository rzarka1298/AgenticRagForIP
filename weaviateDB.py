import os
import time
import sys
import logging
from pathlib import Path

import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
import XMLPatent

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def create_weaviate_client():
    """
    Create and return a Weaviate client connection with retries.
    """
    weaviate_url = os.environ.get("WEAVIATE_URL")
    weaviate_api_key = os.environ.get("WEAVIATE_API_KEY")
    if not weaviate_url or not weaviate_api_key:
        logging.error("Environment variables WEAVIATE_URL and WEAVIATE_API_KEY must be set.")
        sys.exit(1)
    
    # Attempt connection with a retry mechanism
    for attempt in range(3):
        try:
            client = weaviate.connect_to_weaviate_cloud(
                cluster_url=weaviate_url,
                auth_credentials=Auth.api_key(weaviate_api_key),
            )
            if client.is_ready():
                logging.info("Connected to Weaviate successfully on attempt %d.", attempt + 1)
                return client
            else:
                logging.warning("Weaviate client not ready on attempt %d.", attempt + 1)
        except Exception as e:
            logging.exception("Error connecting to Weaviate on attempt %d: %s", attempt + 1, e)
        time.sleep(2)

    logging.error("Failed to connect to Weaviate after multiple attempts.")
    sys.exit(1)

def create_or_get_patents_collection(client):
    """
    Create a 'Patents' collection if it doesn't exist; otherwise, return the existing one.
    """
    schema_name = "Patents"
    try:
        schema = client.schema.get()
        for class_obj in schema.get("classes", []):
            if class_obj["class"] == schema_name:
                logging.info("Collection '%s' already exists. Retrieving it.", schema_name)
                return client.collections.get(schema_name)
    except Exception as e:
        logging.warning("Could not retrieve existing schema: %s", e)
    
    # Create new collection/schema if not found
    try:
        logging.info("Creating collection '%s'...", schema_name)
        patents = client.collections.create(
            name=schema_name,
            vectorizer_config=Configure.Vectorizer.text2vec_weaviate(),  # Uses Weaviate text embeddings
            generative_config=Configure.Generative.cohere(),              # Sets up the Cohere generative integration
            properties=[
                {"name": "title", "dataType": ["text"]},
                {"name": "abstract", "dataType": ["text"]},
                {"name": "description", "dataType": ["text"]}
            ]
        )
        logging.info("Collection '%s' created successfully.", schema_name)
        return patents
    except Exception as e:
        logging.error("Error creating collection '%s': %s", schema_name, e)
        sys.exit(1)

def add_patents_to_collection(collection, batch_size=10):
    """
    Ingest a batch of patent XML files into the Weaviate collection.
    """
    # Retrieve the list of patents and define how many to add for testing
    data = XMLPatent.file_list
    test_db_size = min(batch_size, len(data))
    
    # Define the base directory using pathlib for cross-platform compatibility
    patent_dir = Path(XMLPatent.DIR_PATH)
    
    with collection.batch.dynamic() as batch:
        for i in range(test_db_size):
            name = data[i]
            file_path = patent_dir / f"{name}.xml"
            if not file_path.exists():
                # Attempt alternative file layout if not found
                alternative_path = patent_dir / name / f"{name}.xml"
                file_path = alternative_path if alternative_path.exists() else None
            
            if file_path is None:
                logging.error("File for patent '%s' not found.", name)
                continue

            try:
                patent_data = XMLPatent.parse_patent_xml(str(file_path))
            except Exception as e:
                logging.error("Error parsing XML for patent '%s': %s", name, e)
                continue

            try:
                batch.add_object({
                    "title": patent_data.get("title", ""),
                    "abstract": patent_data.get("abstract", ""),
                    "description": patent_data.get("description", "")
                })
                logging.info("Queued patent '%s' for import.", name)
            except Exception as e:
                logging.exception("Error adding patent '%s' to batch: %s", name, e)
            
            if batch.number_errors > 10:
                logging.error("Batch import stopped due to excessive errors (%d errors).", batch.number_errors)
                break

    if batch.failed_objects:
        logging.warning("Number of failed imports: %d", len(batch.failed_objects))
        if batch.failed_objects:
            logging.warning("First failed object: %s", batch.failed_objects[0])
    else:
        logging.info("All patents imported successfully.")

def main():
    client = create_weaviate_client()
    patents_collection = create_or_get_patents_collection(client)
    
    # Ingest a limited number for testing; increase batch_size once validated
    add_patents_to_collection(patents_collection, batch_size=10)
    
    # Optional: Query a few objects to verify ingestion
    try:
        result = patents_collection.query.get(properties=["title", "abstract"], limit=5)
        logging.info("Sample documents from collection: %s", result)
    except Exception as e:
        logging.error("Error querying sample patents: %s", e)
    
    client.close()  # Cleanly close the connection
    logging.info("Client connection closed.")

if __name__ == "__main__":
    main()
