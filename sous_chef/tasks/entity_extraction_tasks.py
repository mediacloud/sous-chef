import requests
from typing import List, Dict
from collections import Counter
from ..flowatom import FlowAtom
import re
from .utils import lazy_import

yake = lazy_import("yake")
spacy = lazy_import("spacy")
spacy_download = lazy_import("spacy_download")

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
       

@FlowAtom.register("SpacyNER")
class SpacyNER(FlowAtom):
    """
    Use local Spacy instance to perform NER on text. 
    """
    model:str
    _defaults:{
        "model":"en_core_web_sm"
    }

    def inputs(self, text:str):pass
    def outputs(self, entities:List[Dict]):pass

    def task_body(self):
        nlp = spacy_download.load_spacy(self.model)
        
        entities = []

        for row in self.data.itertuples():
            text = row.text
            
            document = nlp(text)

            doc_ents = [{"text": ent.text, "type":ent.label_} for ent in document.ents]

            entities.append(doc_ents)

        self.results.entities = entities
  



@FlowAtom.register("TopNEntities")
class TopNEntities(FlowAtom):
    """
    With an input of many list of dicts, output a list of the top N most commonly occuring entities across all documents
    Top_n limits the number of entities returned, and filter_type limits results to a specific entity-type (ie PER, OBJ, etc)
    Also calculated the percentage of articles each entity appears in of the total- optionally sort by that instead of total counts.
    """
    
    top_n:int
    filter_type:str
    sort_by:str
    _defaults:{
        "top_n":-1,
        "filter_type":"",
        "sort_by":"total"
    }
    
    def inputs(self, entities:List[Dict]):pass
    def outputs(self, top_entities:str, entity_counts:int, entity_appearance_percent:float):pass
    
    @classmethod
    def creates_new_document(self):
        return True   
    
    def task_body(self):
        
        #Total count of entities found
        EntitiesTotalCount = Counter()
        #Only count one appearance of an entity per article, so we can get percentages
        EntitiesAppearedCount = Counter()
        
        for article_entities in self.data.entities:
            
            if isinstance(article_entities, dict):
                #This covers the case where we have just a single entity in an article
                article_entities = [article_entities]
            

            for ent in article_entities:              
                if self.filter_type != "" and ent["type"] != self.filter_type:
                    pass
                
                if not isinstance(ent, dict):
                    pass
                
                #ent_slug = f"{ent['text']}-{ent['type']}"
                if ent['text'] is not None:
                    EntitiesTotalCount.update(Counter({ent['text']:1}))
            
            for ent_text in set([e["text"] for e in article_entities]):
                EntitiesAppearedCount.update(Counter({ent_text:1}))

        number_of_articles = len(self.data.entities)

        if(self.sort_by == "total"):
            sorted_entities, entity_counts = zip(*EntitiesTotalCount.most_common(self.top_n))
            entity_apperances = [EntitiesAppearedCount[e] for e in sorted_entities]
        
        elif(self.sort_by == "percentage"):
            sorted_entities, entity_apperances = zip(*EntitiesAppearedCount.most_common(self.top_n))
            entity_counts = [EntitiesTotalCount[e] for e in sorted_entities]
           
        entity_apperances = [e/number_of_articles for e in entity_apperances]

        self.results.top_entities = sorted_entities
        self.results.entity_counts = entity_counts
        self.results.entity_appearance_percent = entity_apperances


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
        "exclude": ["", '"', "'", "$", "&"]
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

@FlowAtom.register("ExtractKeywords")
class Keywords(FlowAtom):
    """
    Extract keywords from text
    """
    
    top_n:int
    ngram_max:int
    dedup_limit:float
    
    _defaults:{
        "top_n":50,
        "ngram_max":3,
        "dedup_limit":.9
    }
    
    def inputs(self, text:str, language:str):pass
    def outputs(self, keywords:list):pass
    
    def task_body(self):
        
        keywords = []
        for row in self.data.itertuples():
            lan = row.language
           
            extractor = yake.KeywordExtractor(lan=lan, 
                                              n=self.ngram_max, 
                                              dedupLim=self.dedup_limit, 
                                              top=self.top_n, 
                                              features=None)
            
            text = row.text
            kws = extractor.extract_keywords(text)
            print(kws)
            words = [i[0] for i in kws]
            keywords.append(words)
            
        
        self.results.keywords = keywords
    
    
    
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
