import requests
from typing import List, Dict
from ..flowatom import FlowAtom



@FlowAtom.register("APIEntityExtraction")
class ApiEntityExtraction(FlowAtom):
    
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
            
        print(entities)
        self.data.entities = entities