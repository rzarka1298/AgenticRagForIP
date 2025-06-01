import os
import time
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()
import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
from weaviate.classes.config import Property
from weaviate.classes.config import Configure, Property, DataType

import XMLPatent
XMLPatent.DIR_PATH = os.path.join(os.getcwd(), "XML")

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
    schema_name = "PatentProject"

    # 1) List existing class names (list_all() returns a list of strings)
    try:
        existing = client.collections.list_all()
        if schema_name in existing:
            logging.info("Collection '%s' already exists; retrieving it.", schema_name)
            return client.collections.get(schema_name)
    except Exception as e:
        logging.warning("Could not list existing collections: %s", e)

    # 2) Otherwise create it exactly once
    logging.info("Collection '%s' not found; creating it now...", schema_name)
    try:
        patents = client.collections.create(
            name=schema_name,
            vectorizer_config=Configure.Vectorizer.text2vec_weaviate(),
            generative_config=Configure.Generative.cohere(),
            properties=[
                Property(name="title",       data_type=DataType.TEXT),
                Property(name="abstract",    data_type=DataType.TEXT),
                Property(name="description", data_type=DataType.TEXT),
            ],
        )
        logging.info("Collection '%s' created successfully.", schema_name)
        return patents
    except Exception as e:
        logging.error("Failed to create collection '%s': %s", schema_name, e)
        sys.exit(1)


def add_patents_to_collection(collection, batch_size=10):
    """
    Ingest up to `batch_size` patent XML files into the Weaviate collection.
    """
    data = XMLPatent.file_list
    test_db_size = min(batch_size, len(data))
    patent_dir = Path(XMLPatent.DIR_PATH)

    error_names = []
    error_count = 0

    # Properly open the batch context so `batch` has add_object() and number_errors
    with collection.batch.dynamic() as batch:
        for i in range(test_db_size):
            name = data[i]  # e.g. "US1234567-20241217.XML"

            # Try uppercase then lowercase extension
            file_path = patent_dir / name
            if not file_path.exists():
                stem = Path(name).stem
                alt = patent_dir / f"{stem}.xml"
                file_path = alt if alt.exists() else None

            if not file_path or not file_path.exists():
                logging.error("File not found: %s", name)
                error_names.append(name)
                error_count += 1
                continue

            # Parse XML
            try:
                patent_data = XMLPatent.parse_patent_xml(str(file_path))
            except Exception as e:
                logging.error("XML parse error for %s: %s", name, e)
                error_names.append(name)
                error_count += 1
                continue

            # Add to batch
            try:
                batch.add_object({
                    "title":       patent_data.get("title", ""),
                    "abstract":    patent_data.get("abstract", ""),
                    "description": patent_data.get("description", "")
                })
                logging.info("Queued patent '%s' for import.", name)
            except Exception as e:
                logging.error("Batch add error for %s: %s", name, e)
                error_names.append(name)
                error_count += 1

            # Bail out if too many errors
            if batch.number_errors > 10:
                logging.error("Stopping ingestion: >10 errors on batch.")
                break

    # Outside the with-block: batch has been sent
    if error_count:
        logging.warning("Ingestion completed with %d errors.", error_count)
        logging.warning("Examples of failures: %s", error_names[:5])
    else:
        logging.info("All %d patents imported successfully.", test_db_size)


def main():
    client = create_weaviate_client()
    patents_collection = create_or_get_patents_collection(client)
    
    # Ingest a limited number for testing; increase batch_size once validated
    add_patents_to_collection(patents_collection, batch_size=10)
    
    # Optional: Query a few objects to verify ingestion
    try:
        fetch_result = patents_collection.query.fetch_objects(
            limit=5,
            return_properties=["title", "abstract"]
        )
        logging.info("Sample documents (fetch_objects): %s", fetch_result)
    except Exception as e:
        logging.error("Error querying sample patents: %s", e)
    
    client.close()  # Cleanly close the connection
    logging.info("Client connection closed.")

if __name__ == "__main__":
    main()
