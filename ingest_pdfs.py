#!/usr/bin/env python3
import os
import glob
import hashlib
from dotenv import load_dotenv

import weaviate
from weaviate.classes.init import Auth

# ─── llama-index (v0.10.x) imports ───────────────────────────────────────────────
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import Settings, VectorStoreIndex, StorageContext, SimpleDirectoryReader
from llama_index.vector_stores.weaviate import WeaviateVectorStore

# ─── CONFIGURATION ────────────────────────────────────────────────────────────────
PDF_DIR        = "./pdfs"                 # Local folder containing PDF files
WEAVIATE_CLASS = "PDFDocument"            # Existing Weaviate class name
EMBED_MODEL    = "text-embedding-ada-002"  # OpenAI embedding model
CHUNK_SIZE     = 512                      # ~512-token chunks
# ────────────────────────────────────────────────────────────────────────────────


def compute_file_hash(path: str) -> str:
    """
    Compute an MD5 hash for the file at `path` for deduplication.
    """
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            hasher.update(block)
    return hasher.hexdigest()


def connect_to_weaviate() -> weaviate.WeaviateClient:
    """
    Connect to Weaviate Cloud using weaviate.connect_to_weaviate_cloud(...)
    with skip_init_checks=True to bypass gRPC health-check issues.
    """
    load_dotenv()
    weaviate_url = os.getenv("WEAVIATE_URL", "").strip()
    weaviate_api_key = os.getenv("WEAVIATE_API_KEY", "").strip()

    if not weaviate_url:
        raise RuntimeError("Missing WEAVIATE_URL in environment.")

    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=weaviate_url,
        auth_credentials=Auth.api_key(weaviate_api_key) if weaviate_api_key else None,
        skip_init_checks=True,
    )


    return client


def fetch_existing_hashes(collection: weaviate.collections.Collection) -> set[str]:
    """
    Fetch existing file_hash values from the given collection via Collections API.
    Returns a set of file_hash strings.
    """
    existing_hashes: set[str] = set()
    try:
        resp = collection.query.fetch_objects(limit=5000)
        for obj in resp.objects or []:
            props = obj.properties or {}
            h = props.get("file_hash")
            if h:
                existing_hashes.add(h)
    except Exception:
        pass
    return existing_hashes


def ingest_and_upsert(client: weaviate.WeaviateClient) -> VectorStoreIndex:
    """
    1) Retrieve the existing WEAVIATE_CLASS collection
    2) Deduplicate by file_hash
    3) Chunk PDFs, embed, and insert each chunk to Weaviate
    4) Build and return a VectorStoreIndex over that collection
    """
    # 1) Connect directly to the existing collection
    collection = client.collections.get(WEAVIATE_CLASS)

    # 2) Fetch existing hashes
    existing_hashes = fetch_existing_hashes(collection)

    # 3) Locate PDFs to ingest
    pdf_paths = glob.glob(os.path.join(PDF_DIR, "*.pdf"))
    if not pdf_paths:
        print(f"⚠️ No PDFs found in '{PDF_DIR}'.")
        client.close()
        return None

    to_process: list[tuple[str, str]] = []
    for path in pdf_paths:
        h = compute_file_hash(path)
        if h not in existing_hashes:
            to_process.append((path, h))

    if not to_process:
        print("✅ No new PDFs to ingest.")
        client.close()
        return None

    print(f"🗂️ Ingesting {len(to_process)} new PDF(s):")
    for p, _ in to_process:
        print("  •", os.path.basename(p))

    # 4) Configure embedding model
    embed_model = OpenAIEmbedding(
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        model=EMBED_MODEL,
        normalize=True,
    )
    Settings.embed_model = embed_model
    Settings.chunk_size = CHUNK_SIZE

    # 5) For each new PDF: chunk → embed → insert
    for pdf_path, file_hash in to_process:
        reader = SimpleDirectoryReader(input_files=[pdf_path])
        docs = reader.load_data()
        for doc in docs:
            content_text = doc.text
            fname = os.path.basename(pdf_path)
            # Compute embedding
            embedding = embed_model.get_text_embedding(content_text)
            # Insert into Weaviate
            data_obj = {
                "content":   content_text,
                "file_name": fname,
                "file_hash": file_hash,
            }
            collection.data.insert(
                data_obj,
                vector=embedding,
            )

    print("✅ Upsert complete.")

    # 6) Build a VectorStoreIndex over the existing collection
    vector_store = WeaviateVectorStore(
        weaviate_client=client,
        index_name=WEAVIATE_CLASS,
        text_key="content",
    )
    index = VectorStoreIndex.from_vector_store(
        vector_store=vector_store,
        storage_context=StorageContext.from_defaults(vector_store=vector_store),
    )
    return index

'''
def verify_collection(collection: weaviate.collections.Collection):
    """
    Fetch and display a few objects from the collection using Collections API.
    """
    resp = collection.query.fetch_objects(limit=3, include_vector=True)
    items = resp.objects or []
    print(f"📊 Retrieved {len(items)} object(s):")
    for obj in items:
        props = obj.properties or {}
        fname = props.get("file_name")
        fhash = props.get("file_hash")
        preview = (props.get("content") or "")[:50] + "..."
        vector = obj.vector or []
        print(f" • {fname} ({fhash}), content preview: {preview}")
        print(f"   embedding (first 5 dims): {vector[:5]}")
'''

def main():
    client = connect_to_weaviate()
    collection = client.collections.get(WEAVIATE_CLASS)
    index = ingest_and_upsert(client)
    client.close()


if __name__ == "__main__":
    main()
