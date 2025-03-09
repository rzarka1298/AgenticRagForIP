import os
import uuid
from termcolor import cprint
from llama_stack_client.lib.agents.agent import Agent
from llama_stack_client.lib.agents.event_logger import EventLogger
from llama_stack_client.types import Document
import glob
import XMLPatent

# Function to load patent XML files from a folder
# def load_patent_documents(folder_path):
#     xml_files = glob.glob(os.path.join(folder_path, '*.XML'))
#     documents = []
#     for file_path in xml_files:
#         with open(file_path, 'r', encoding='utf-8') as file:
#             content = file.read()
#         documents.append(
#             Document(
#                 document_id=os.path.basename(file_path),
#                 content=content,
#                 mime_type="text/xml",
#                 metadata={}
#             )
#         )
#     return documents

def load_patent_documents(folder):
    xml_files = XMLPatent.file_list[0:3]
    documents = []
    for file in xml_files:
        loc = rf"{folder}/{file}"
        patent_info = XMLPatent.parse_patent_xml(loc)
        documents.append(
            Document( document_id=os.path.basename(loc),
                     content = f'TITLE:{patent_info["title"]}ABSTRACT:{patent_info["abstract"]}DESCRIPTION:{patent_info["description"]}',
                     mime_type="text",
                     metadata={}
            )
        )
    return documents

def create_http_client():
    from llama_stack_client import LlamaStackClient

    return LlamaStackClient(
        base_url=f"http://localhost:{os.environ['LLAMA_STACK_PORT']}"
    )


def create_library_client(template="together"):
    from llama_stack import LlamaStackAsLibraryClient

    client = LlamaStackAsLibraryClient(template)
    client.initialize()
    return client


client = (
    create_library_client()
)  # or create_http_client() depending on the environment you picked



# Step 2: Load patent documents from the local 'patents' folder
folder = "/Users/rugvedzarkar/Desktop/PatentMar8/XML"  # Update to your actual folder path
documents = load_patent_documents(folder)
print(f"Loaded {len(documents)} patent documents.")

vector_providers = [
    provider for provider in client.providers.list() if provider.api == "vector_io"
]
provider_id = vector_providers[0].provider_id  # Use the first available vector provider
vector_db_id = f"patent-vector-db-{uuid.uuid4().hex}"
client.vector_dbs.register(
    vector_db_id=vector_db_id,
    provider_id=provider_id,
    embedding_model="all-MiniLM-L6-v2",
    embedding_dimension=384,
)

client.tool_runtime.rag_tool.insert(
    documents=documents,
    vector_db_id=vector_db_id,
    chunk_size_in_tokens=512,
)


# Step 5: Create the RAG agent configured for patent analysis
rag_agent = Agent(
    client,
    model=os.environ["INFERENCE_MODEL"],
    instructions="You are a patent expert assistant. Use the provided documents to answer questions about patents accurately.",
    enable_session_persistence=False,
    tools=[
        {
            "name": "builtin::rag/knowledge_search",
            "args": {"vector_db_ids": [vector_db_id]},
        }
    ],
)

# Step 6: Create a session and run a test query
session_id = rag_agent.create_session("patent-session")
user_prompts = [
    "give me a 3 paragraph summary of the patent titled: Area registration method and area registration system. Use the knowledge_search tool to gather details."
]

for prompt in user_prompts:
    cprint(f"User> {prompt}", "green")
    response = rag_agent.create_turn(
        messages=[{"role": "user", "content": prompt}],
        session_id=session_id,
    )
    for log in EventLogger().log(response):
        log.print()
