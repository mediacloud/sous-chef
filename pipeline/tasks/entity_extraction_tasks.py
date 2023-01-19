import requests
from typing import List, Dict
from ..flowatom import FlowAtom



@FlowAtom.register("APIEntityExtraction")
class ApiEntityExtraction(FlowAtom):
    """
    Use an external news-entity-server API endpoint to extract all the entities from an input text.
    """
    
    def inputs(self, text:str, language:str):pass
    def outputs(self, entities:List[Dict]):pass
    
    def task_body(self):

        ENTITY_SERVER_URL = "https://news-entity-server.dfprod.dataplusfeminism.mit.edu/"
        target_url = ENTITY_SERVER_URL + 'entities/from-content'
        
        entities = []
        entity_types = []
        for row in self.data.itertuples():
            text = row.text
            language = row.language
            response = requests.post(target_url, data=dict(text=row.text, language=row.language, url="http://foo.com")).json()
            if(response["status"] == "ok"):
                these_entities = response['results']["entities"]
                parsed_down = [{"text":e["text"], "type":e["type"]} for e in these_entities]
                entities.append(parsed_down)
                
            else:
                entities.append({"text":None, "type":None})
            
        
        self.results.entities = entities
        
@FlowAtom.register("TopNEntities")
class TopNEntities(FlowAtom):
    """
    With an input of many list of dicts, output a list of the top N most commonly occuring entities.
    Top_n limits the number of entities returned, and filter_type limits results to a specific entity-type (ie PER, OBJ, etc)
    """
    
    top_n:int
    filter_type:str
    _defaults:{
        "top_n":-1,
        "filter_type":""
    }
    
    def inputs(self, entities:List[Dict]):pass
    def outputs(self, top_entities:list):pass
    
    @classmethod
    def creates_new_document(self):
        return True   
    
    def task_body(self):
        
        counter = {}
        
        for article_entities in self.data.entities:
            print(article_entities)
            if isinstance(article_entities, dict):
                article_entities = [article_entities]
            for ent in article_entities:
                print(ent)
                
                if self.filter_type != "" and ent["type"] != self.filter_type:
                    pass
                
                if not isinstance(ent, dict):
                    pass
                
                #ent_slug = f"{ent['text']}-{ent['type']}"
                ent_slug = ent['text']
                if ent_slug is not None:
                
                    if ent_slug in counter:
                        counter[ent_slug] += 1
                    else: 
                        counter[ent_slug] = 1
        

        top_entities = sorted(counter)
        if self.top_n > 0:
            top_entities = top_entities[:self.top_n]
        
        
        self.results.top_entities = top_entities
       
        
        