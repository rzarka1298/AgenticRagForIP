import os
import uuid
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from llama_stack_client.lib.agents.agent import Agent
from llama_stack_client.lib.agents.event_logger import EventLogger
from llama_stack_client.types import Document
import XMLPatent

# Global variables to hold our client, agent, session, etc.
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
    xml_files = XMLPatent.file_list[0:10]
    documents = []
    for file in xml_files:
        curLoc = rf"{XMLPatent.DIR_PATH}/{file}"
        if os.path.exists(curLoc):
            loc = curLoc
        else:
            stripped_curLoc = curLoc.strip('.XML')
            loc = rf"{stripped_curLoc}/{file}"
        patent_info = XMLPatent.parse_patent_xml(loc)
        documents.append(
            Document(
                document_id=patent_info["meta-data"]["ID"],
                content=(
                    f'TITLE:{patent_info["title"]}, '
                    f'ABSTRACT:{patent_info["abstract"]}, '
                    f'DESCRIPTION:{patent_info["description"]}'
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

    # Initialize the client asynchronously
    client = await create_library_client()

    # Load patent documents (update the folder path as needed)
    folder = r"/Users/rugvedzarkar/Desktop/Patent+React/XML"
    documents = load_patent_documents(folder)
    print(f"Loaded {len(documents)} patent documents.")

    # Offload providers.list() to a separate thread to avoid nested event loop issues
    providers_list = await asyncio.to_thread(client.providers.list)
    vector_providers = [provider for provider in providers_list if provider.api == "vector_io"]
    provider_id = vector_providers[0].provider_id

    vector_db_id = f"patent-vector-db-{uuid.uuid4().hex}"
    # Register the vector DB in a separate thread
    await asyncio.to_thread(
        client.vector_dbs.register,
        vector_db_id=vector_db_id,
        provider_id=provider_id,
        embedding_model="all-MiniLM-L6-v2",
        embedding_dimension=384,
    )

    # Insert documents into the vector DB using a separate thread
    await asyncio.to_thread(
        client.tool_runtime.rag_tool.insert,
        documents=documents,
        vector_db_id=vector_db_id,
        chunk_size_in_tokens=512,
    )

    # Create the RAG agent in a separate thread to avoid asyncio.run conflicts
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
    # Similarly, create a session in a separate thread
    session_id = await asyncio.to_thread(lambda: rag_agent.create_session("patent-session"))
    print("Agent and session initialized.")

    yield  # Lifespan startup complete; application is now running.

    # (Optional) Add shutdown logic here if needed.

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

# Query endpoint – blocking agent calls are offloaded to a separate thread
@app.post("/query")
async def query_endpoint(request: QueryRequest):
    global rag_agent, session_id
    response = await asyncio.to_thread(
        lambda: rag_agent.create_turn(
            messages=[{"role": "user", "content": request.user_query}],
            session_id=session_id,
        )
    )
    response_text = await asyncio.to_thread(
        lambda: "".join(str(log) + "\n" for log in EventLogger().log(response))

    )
    return {"response": response_text.strip()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
