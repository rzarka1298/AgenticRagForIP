import xml.dom.minidom
import os
DIR_PATH = r"/Users/rugvedzarkar/Desktop/PatentMar8/XML"
file_list = os.listdir(DIR_PATH)


#function to use to collect info from a single patent
def parse_patent_xml(loc: str):
    """parses an xml patent at specified location on params.\nReturns dict of patent info including metadata.\n
    Inds for dict: "description", "abstract", "title", "meta-data".\nSee function for more inds for meta-data"""
    domTree = xml.dom.minidom.parse(loc)
    head = domTree.documentElement
    desc = pullDesc(headN=head)
    abstract = pullAbs(headN = head)
    title = pullTitle(headN = head)
    metadata = pullMeta(headN = head)
    return {
        "description": desc,
        "abstract": abstract,
        "title": title,
        "meta-data": metadata
    }


#####################################################
###   helper functions to pull info from files    ###
#####################################################
def pullDesc(headN):
    """returns str of all text from description node in specified file"""
    descN = headN.getElementsByTagName("description")[0].childNodes
    desc = ""
    for i in range(len(descN)):
        # print(f'{descN[i]}.....{descN[i].nodeType}')    
        if (descN[i].nodeType==1 and descN[i].childNodes[0].nodeValue is not None):
            if (descN[i].getAttribute("id") and descN[i].getAttribute("id")[0]=="h"):
                desc+=descN[i].childNodes[0].nodeValue.strip()+": "  #consider adding colon here for llm input clarity improvement
            elif(descN[i].getAttribute("id") and descN[i].getAttribute("id")[0]=="p"):
                desc+=descN[i].childNodes[0].nodeValue.strip()+" "
    return desc.strip()


def pullAbs(headN):
    """returns str of all text which make the abstract ndoe in specified file"""
    abstract = ""
    for p in headN.getElementsByTagName("abstract")[0].getElementsByTagName("p"):
        abstract+=str(p.childNodes[0].nodeValue).strip()+" "
    return abstract.strip()
def pullTitle(headN):
    """returns invention title of the specified patent file"""
    return headN.getElementsByTagName("invention-title")[0].childNodes[0].nodeValue


def pullMeta(headN):
    """collects metadata from file and returns in dict format"""
    metadata = {}
    metadata["ID"] = headN.getAttribute("file").strip(".XML")
    return metadata
#####################################################
#####################################################
