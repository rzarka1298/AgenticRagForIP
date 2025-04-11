import os
import uuid
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from llama_stack_client.lib.agents.agent import Agent
from llama_stack_client.lib.agents.event_logger import EventLogger
from llama_stack_client.types import Document
import XMLPatent

# Global variables for our client, agent, session, and vector DB ID
client = None
rag_agent = None
session_id = None
vector_db_id = None

# Asynchronous client initialization helper
async def create_library_client(template="together"):
    from llama_stack import LlamaStackAsLibraryClient
    client = LlamaStackAsLibraryClient(template)
    await client.async_client.initialize()
    return client

# Synchronous function to load patent documents from a folder
def load_patent_documents(folder):
    # Here, we still load the documents from a local folder just once,
    # but they will be inserted into Weaviate so that retrieval uses Weaviate.
    xml_files = XMLPatent.file_list[0:10]
    documents = []
    for file in xml_files:
        # Use forward slash for OS-agnostic paths or use pathlib if desired.
        curLoc = f"{XMLPatent.DIR_PATH}/{file}"
        if os.path.exists(curLoc):
            loc = curLoc
        else:
            stripped_curLoc = curLoc.strip('.XML')
            loc = f"{stripped_curLoc}/{file}"
        try:
            patent_info = XMLPatent.parse_patent_xml(loc)
        except Exception as e:
            print(f"Error parsing file {loc}: {e}")
            continue
        documents.append(
            Document(
                document_id=patent_info["meta-data"]["ID"],
                content=(
                    f'TITLE: {patent_info["title"]}, '
                    f'ABSTRACT: {patent_info["abstract"]}, '
                    f'DESCRIPTION: {patent_info["description"]}'
                ),
                mime_type="text",
                metadata={}
            )
        )
    return documents

# Lifespan event handler using an async context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, rag_agent, session_id, vector_db_id

    # 1. Initialize the LlamaStack client asynchronously
    client = await create_library_client()

    # 2. Load patent documents from the local folder (source documents)
    folder = r"/Users/rugvedzarkar/Desktop/Patent+React/XML"
    documents = load_patent_documents(folder)
    print(f"Loaded {len(documents)} patent documents.")

    # 3. Retrieve available vector DB providers.
    # Update the filtering: select the provider whose API identifies Weaviate
    providers_list = await asyncio.to_thread(client.providers.list)
    weaviate_providers = [provider for provider in providers_list if provider.api.lower() == "weaviate"]
    if not weaviate_providers:
        raise Exception("No Weaviate provider found in the provider list.")
    provider_id = weaviate_providers[0].provider_id

    # 4. Register the vector DB with Weaviate as the memory.
    # Note: This vector_db_id will be used by the RAG agent for retrieval.
    vector_db_id = f"patent-vector-db-{uuid.uuid4().hex}"
    await asyncio.to_thread(
        client.vector_dbs.register,
        vector_db_id=vector_db_id,
        provider_id=provider_id,
        embedding_model="all-MiniLM-L6-v2",
        embedding_dimension=384,
    )

    # 5. Insert the loaded documents into the Weaviate-backed vector DB.
    await asyncio.to_thread(
        client.tool_runtime.rag_tool.insert,
        documents=documents,
        vector_db_id=vector_db_id,
        chunk_size_in_tokens=512,
    )

    # 6. Create the RAG agent and configure it to use the registered Weaviate DB.
    rag_agent = await asyncio.to_thread(lambda: Agent(
        client=client,
        model=os.environ["INFERENCE_MODEL"],
        instructions=(
            "You are a patent expert assistant. Use the provided documents to answer questions about patents accurately. "
            "Always respond in clear, fluent English, and do not include any markup tags like <inference> in your answer."
        ),
        enable_session_persistence=False,
        tools=[
            {
                "name": "builtin::rag/knowledge_search",
                "args": {"vector_db_ids": [vector_db_id]},
            }
        ]
    ))
    # 7. Create a session for the agent
    session_id = await asyncio.to_thread(lambda: rag_agent.create_session("patent-session"))
    print("Agent and session initialized.")

    yield  # Startup complete; application is now running.

    # (Optional) Add shutdown/cleanup logic here if needed.

# Create the FastAPI app using the lifespan handler
app = FastAPI(lifespan=lifespan)

# Allow all origins during development (adjust as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request model for the query endpoint
class QueryRequest(BaseModel):
    user_query: str

# Query endpoint – offload blocking agent calls to a separate thread
@app.post("/query")
async def query_endpoint(request: QueryRequest):
    global rag_agent, session_id
    if not request.user_query:
        raise HTTPException(status_code=400, detail="Query cannot be empty")
    try:
        response = await asyncio.to_thread(
            lambda: rag_agent.create_turn(
                messages=[{"role": "user", "content": request.user_query}],
                session_id=session_id,
            )
        )
        response_text = await asyncio.to_thread(
            lambda: "".join(str(log) + "\n" for log in EventLogger().log(response))
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"response": response_text.strip()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
