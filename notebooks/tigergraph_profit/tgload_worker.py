import json
import os
import requests
from requests.auth import HTTPBasicAuth
from collections import defaultdict
from typing import List
from db_client import DorisClient

class Edge(object):
    def __init__(self,entities):
        self.entities = entities

    def add_edge_entities(self,edge_dict):
        self.entities = entities
        
        return entities


class Vertex(object):
    def __init__(self,entities):
        self.entities = entities
        
    def add_vertex_entities(self,edge_dict):
        return entities
    

class BaseDorisExtract(DorisClient):
    def __init__(self,consur):
        self.consur = consur

    def 
    
    
    
class tgDataProcessor(object):
    def __init__(self,row_dict):
        self.row_dict = row_dict
        self.vertex_dict = defaultdict(dict)
        self.edge_dict = defaultdict(dict)
        
    def parse_vertex_row(self,row_dict,primary_id):
        vertex_dict = self.vertex_dict
        
    def parse_edge_row(self,row_dict,source_type,source_id,edge_type,target_type,target_id,attributes)
        edge_row_dict = self.entities[source_type]
        edge_row_dict["source_id"] = {edge_type:{target_type:{target_id:{}}}}
        return edge_row_dict
