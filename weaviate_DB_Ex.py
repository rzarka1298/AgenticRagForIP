import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.config import Configure
import os
import XMLPatent

#required weaviate provider info:
weaviate_url = os.environ["WEAVIATE_URL"]
weaviate_api_key = os.environ["WEAVIATE_API_KEY"]


client = weaviate.connect_to_weaviate_cloud(
    cluster_url=weaviate_url,
    auth_credentials=Auth.api_key(weaviate_api_key),
)

print(client.is_ready())  # Should print: "True"


patents = client.collections.create(
    name="Patents",
    vectorizer_config=Configure.Vectorizer.text2vec_weaviate(), # Configure the Weaviate Embeddings integration
    generative_config=Configure.Generative.cohere()             # Configure the Cohere generative AI integration
)

patents = client.collections.get("Patents")

#info needed before adding patents to the list
data = XMLPatent.file_list
test_db_size = 10 #number of patents to add to the weaviate db for testing

#adds patents to the db
with patents.batch.dynamic() as batch:
    for i in range(test_db_size):
        name = data[i]
        curLoc = rf"{XMLPatent.DIR_PATH}\{name}.xml"
        if (os.path.exists(curLoc)):
            loc = curLoc
        else:
            loc = rf"{curLoc.strip(".xml")}\{name}.xml"
        d = XMLPatent.parse_patent_xml(loc)
        batch.add_object({
            "title": d["title"],
            "abstract": d["abstract"],
            "description": d["description"]
        })
        if batch.number_errors > 10:
            print("Batch import stopped due to excessive errors.")
            break

failed_objects = patents.batch.failed_objects
if failed_objects:
    print(f"Number of failed imports: {len(failed_objects)}")
    print(f"First failed object: {failed_objects[0]}")



client.close()  # Free up resources