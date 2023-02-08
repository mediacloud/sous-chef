import requests
from typing import List, Dict
from ..flowatom import FlowAtom
import re


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
            
            if isinstance(article_entities, dict):
                #This covers the case where we have just a single entity
                article_entities = [article_entities]
            
            for ent in article_entities:              
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

@FlowAtom.register("SimpleTokenizeTask")
class SimpleTokens(FlowAtom):
    """
    Split strings into a list of tokens. "seps" defines what separators in addition to spaces to use as delimiters.
    Tokens in "exclude" are not returned
    """
    
    seps:str
    exclude:list
    _defaults:{
        "seps":",.?!",
        "exclude": ["", '"', "'"]
    }
    
    def inputs(self, text:str):pass
    def outputs(self, tokens:list):pass
    
    def task_body(self):
        
        tokens = []
        for string_to_split in self.data.text:
            if string_to_split is not None:
                string_to_split = str(string_to_split) #just in case
                these_tokens = string_to_split.replace(self.seps, " ").split(" ")
                these_tokens = list(filter(lambda x:x not in self.exclude, these_tokens))
                tokens.append(these_tokens)
            else:
                tokens.append([])
            
        self.results.tokens = tokens
    

@FlowAtom.register("ExtractByRegex")
class ExtractRegexMatch(FlowAtom):
    """
    General Regex matching class. Subclassed for various specialized extractors
    """
    regex:str

    _defaults:{
        'regex':"",

    }
        
    def inputs(self, text:str):pass
    def outputs(self, matches:list):pass

    def get_regex(self):
        return None
    
    def task_body(self):
        if self.get_regex() is not None:
            self.regex = self.get_regex()
            
        matches = []
        for string_to_search in self.data.text:
            if string_to_search is not None:
                string_to_search = str(string_to_search)
                these_matches = re.findall(self.regex, string_to_search)
                matches.append(these_matches)
            else:
                matches.append([])
        
        self.results.matches = matches

#Extract hashtags
@FlowAtom.register("ExtractHashtags")
class ExtractHashtags(ExtractRegexMatch):
    """
    Extract all hashtags from provided strings
    """
    def get_regex(self):
        return '[#]{1}[\S]+'
    

#Extract URLS
@FlowAtom.register("ExtractURLS")
class ExtractURLS(ExtractRegexMatch):
    """
    Extract all URLS from provided strings
    """
    #This is just the most straightforward url regex I've ever come across- still a monster, though. 
    def get_regex(self):
        url_regex = "(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})"
        return url_regex