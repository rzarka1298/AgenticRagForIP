#!/usr/bin/env python3
import os
import glob
import hashlib
import json
from dotenv import load_dotenv

import weaviate
from llama_index import (
    SimpleDirectoryReader,
    ServiceContext,
    GPTVectorStoreIndex,
)
from llama_index.vector_stores import WeaviateVectorStore

# ─── CONFIGURATION ────────────────────────────────────────────────────────────
# (1) Local folder where your PDFs live
PDF_DIR = "./pdfs"

# (2) Weaviate class name that will hold all PDF‐chunk objects
WEAVIATE_CLASS = "PDFDocument"

# (3) Embedding model you want to use. 
#     “text-embedding-ada-002” is recommended for best retrieval quality.
EMBED_MODEL = "text-embedding-ada-002"
CHUNK_SIZE  = 512   # ~512 tokens per chunk

# (4) How many chunks returned on a query? (used eventually in your RAG agent)
TOP_K = 10

# ───────────────────────────────────────────────────────────────────────────────


def compute_file_hash(path: str) -> str:
    """
    Compute an MD5 hash for a file at `path`.
    We’ll use this to detect duplicates in Weaviate.
    """
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            hasher.update(block)
    return hasher.hexdigest()


def connect_to_weaviate() -> weaviate.Client:
    """
    Initialize a Weaviate client using WEAVIATE_URL and WEAVIATE_API_KEY
    from your environment. If the class WEAVIATE_CLASS doesn’t exist,
    create a minimal schema for it.
    """
    url     = os.getenv("WEAVIATE_URL", "").strip()
    api_key = os.getenv("WEAVIATE_API_KEY", "").strip()

    if not url or not api_key:
        raise RuntimeError(
            "Please set WEAVIATE_URL and WEAVIATE_API_KEY in your environment (or .env)."
        )

    client = weaviate.Client(
        url=url,
        auth_client_secret=weaviate.AuthApiKey(api_key),
        additional_headers={"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY", "")},
    )

    # Check if our class already exists; if not, create it.
    existing_classes = [c["class"] for c in client.schema.get()["classes"]]
    if WEAVIATE_CLASS not in existing_classes:
        class_schema = {
            "class": WEAVIATE_CLASS,
            "vectorizer": "none",  # we’ll push precomputed embeddings
            "properties": [
                {"name": "content",   "dataType": ["text"]},
                {"name": "file_name", "dataType": ["string"]},
                {"name": "file_hash", "dataType": ["string"]},
            ],
        }
        client.schema.create_class(class_schema)

    return client


def fetch_existing_hashes(client: weaviate.Client) -> set[str]:
    """
    Query Weaviate for all existing 'file_hash' values in WEAVIATE_CLASS.
    Returns a Python set of those hashes.
    """
    existing = set()
    # We page through all objects, requesting only the 'file_hash' property.
    query = client.query.get(WEAVIATE_CLASS, ["file_hash"]).with_additional(
        "id"
    )  # grabbing IDs lets us page
    res = query.do()
    if "data" not in res or "Get" not in res["data"]:
        return existing

    objs = res["data"]["Get"].get(WEAVIATE_CLASS, [])
    for obj in objs:
        h = obj.get("file_hash")
        if h:
            existing.add(h)

    # Check if there are more pages (cursor‐based pagination).
    # If there’s a “cursor” in the response, keep fetching.
    cursor = res.get("data", {}).get("Get", {}).get("_additional", {}).get("cursor")
    while cursor:
        res = (
            client.query.get(WEAVIATE_CLASS, ["file_hash"])
            .with_additional("id")
            .with_additional({"after": cursor})
            .do()
        )
        objs = res["data"]["Get"].get(WEAVIATE_CLASS, [])
        for obj in objs:
            h = obj.get("file_hash")
            if h:
                existing.add(h)
        cursor = res.get("data", {}).get("Get", {}).get("_additional", {}).get("cursor", None)

    return existing


def main():
    # ─── Load environment variables (WEAVIATE_URL, WEAVIATE_API_KEY, OPENAI_API_KEY) ───
    load_dotenv()

    # ─── Connect to Weaviate, ensure the class exists ────────────────────────────────
    client = connect_to_weaviate()

    # ─── Fetch all file_hashes already ingested ──────────────────────────────────────
    existing_hashes = fetch_existing_hashes(client)
    print(f"🔍 Found {len(existing_hashes)} file(s) already in Weaviate.")

    # ─── List all PDF files in PDF_DIR ───────────────────────────────────────────────
    pdf_paths = glob.glob(os.path.join(PDF_DIR, "*.pdf"))
    if not pdf_paths:
        print(f"⚠️  No PDFs found in {PDF_DIR}. Exiting.")
        return

    # ─── Determine which PDFs are “new” (not yet in Weaviate) ────────────────────────
    to_process: list[tuple[str, str]] = []  # (path, file_hash)
    for path in pdf_paths:
        h = compute_file_hash(path)
        if h in existing_hashes:
            # Skip anything whose MD5 hash is already stored.
            continue
        to_process.append((path, h))

    if not to_process:
        print("✅ No new PDFs to ingest. Exiting.")
        return

    print(f"🗂️  Will ingest {len(to_process)} new file(s):")
    for p, _ in to_process:
        print("   •", os.path.basename(p))

    # ─── Parse PDFs into llama_index Documents ──────────────────────────────────────
    all_docs = []
    for pdf_path, file_hash in to_process:
        # SimpleDirectoryReader supports PDF parsing out of the box.
        reader = SimpleDirectoryReader(input_files=[pdf_path])
        docs = reader.load_data()
        for d in docs:
            # Attach file-level metadata so we can filter later on 
            d.metadata["file_name"] = os.path.basename(pdf_path)
            d.metadata["file_hash"] = file_hash
            all_docs.append(d)

    # ─── Build a ServiceContext that uses OpenAI's ada-002 embeddings ───────────────
    service_context = ServiceContext.from_defaults(
        chunk_size=CHUNK_SIZE,
        embed_model=EMBED_MODEL,
        embed_model_kwargs={"normalize": True},  # recommended for OpenAI embeddings
    )

    # ─── Set up a WeaviateVectorStore to handle vector insertion ────────────────────
    vector_store = WeaviateVectorStore(
        client=client,
        class_name=WEAVIATE_CLASS,
        text_key="content",
    )

    # ─── Upsert (insert-or-update) all new docs into Weaviate ───────────────────────
    GPTVectorStoreIndex.from_documents(
        all_docs,
        service_context=service_context,
        vector_store=vector_store,
        upsert=True,  # ensures no duplicate vectors if re-run mid-ingestion
    )

    print(f"✅ Successfully ingested {len(to_process)} PDF(s) into Weaviate.")
    print("   You can now run your RAG agent against the updated index.")
    return


if __name__ == "__main__":
    main()
