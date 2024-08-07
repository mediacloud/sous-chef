import requests
from typing import List, Dict, Union
from collections import Counter
from ..flowatom import FlowAtom
import re
from .utils import lazy_import
from uniseg.wordbreak import words as segwords
import mc_providers

yake = lazy_import("yake")
spacy = lazy_import("spacy")
spacy_download = lazy_import("spacy_download")
spacy_ngram = lazy_import("spacy_ngram")

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
            
            if self.filter_type != "":
                article_entities = [e for e in article_entities if e["type"] == self.filter_type]

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
        
        if(len(EntitiesTotalCount) > 1):

            if(self.sort_by == "total"):
                sorted_entities, entity_counts = zip(*EntitiesTotalCount.most_common(self.top_n))
                entity_apperances = [EntitiesAppearedCount[e] for e in sorted_entities]
            
            elif(self.sort_by == "percentage"):
                sorted_entities, entity_apperances = zip(*EntitiesAppearedCount.most_common(self.top_n))
                entity_counts = [EntitiesTotalCount[e] for e in sorted_entities]
        
            entity_apperances = [e/number_of_articles for e in entity_apperances]

        else:
            sorted_entities = []
            entity_counts = []
            entity_apperances = []
       
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
    Extract keywords from text, using the YAKE keyword extractor. 
    top_n, ngram_max, and dedup_limit are all hyperparameters which shouldn't really have to be tuned
    from the provided defaults, but ymmv. Refer to the YAKE source for more details. 
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
            words = [i[0] for i in kws]
            keywords.append(words)
             
        self.results.keywords = keywords
    
@FlowAtom.register("TopKeywords")
class TopNKeywords(FlowAtom):
    """
    Collate the top-n keywords from a list of keywords-per-article. 
    Sort by either "total" or "percentage" 
    """
    top_n: int
    sort_by: str
    _defaults: {
        "top_n":-1,
        "sort_by":"total"
    }

    @classmethod
    def creates_new_document(self):
        return True  

    def inputs(self, keywords:list):pass
    def outputs(self, top_keywords:str, keyword_counts:int, keyword_appearance_percent:float):pass

    def task_body(self):
         #Total count of entities found
        TotalCount = Counter()
        #Only count one appearance of a keyword per article, so we can get percentages
        AppearedCount = Counter()

        for article_keywords in self.data.keywords:

            for keyword in article_keywords:              
                TotalCount.update(Counter({keyword:1}))
            
            for keyword in set(article_keywords):
                AppearedCount.update(Counter({keyword:1}))

        number_of_articles = len(self.data.keywords)

        if(self.sort_by == "total"):

            sorted_kws, kw_counts = zip(*TotalCount.most_common(self.top_n))
            kw_apperances = [AppearedCount[e] for e in sorted_kws]
        
        elif(self.sort_by == "percentage"):
            sorted_kws, kw_apperances = zip(*AppearedCount.most_common(self.top_n))
            kw_counts = [TotalCount[e] for e in sorted_kws]
           
        kw_apperances = [e/number_of_articles for e in kw_apperances]

        self.results.top_keywords = sorted_kws
        self.results.keyword_counts = kw_counts
        self.results.keyword_appearance_percent = kw_apperances

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

@FlowAtom.register("SpacyNGram")
class NGrams(FlowAtom):
    """
    Obtains gram_size-grams from text
    """
    model:str
    gram_size:int #For compatability with mixins while issue#10 is active
    _defaults:{
        "model":"en_core_web_sm",   
        "gram_size":2
    }


    def inputs(self, text:str):pass
    def outputs(self, ngrams: list):pass

    def validate(self):
        self.gram_size = int(self.gram_size)

    def task_body(self):
        from spacy_ngram import NgramComponent
        nlp = spacy_download.load_spacy(self.model)
        nlp.add_pipe('spacy-ngram', config={
            'ngrams': (1, self.gram_size)
        })
        
        ngrams = []


        for row in self.data.itertuples():
            text = row.text
            
            document = nlp(text)

            ngrams.append(document._.get(f"ngram_{self.gram_size}"))
            
            
        self.results.ngrams = ngrams

@FlowAtom.register("TopTerms")
class TopTerms(FlowAtom):
    """
    Simple top-terms using uniseg
    """
    top_n:int
    _defaults = {
        "top_n":100
    }

    def inputs(self, text:str, language:str):pass
    def outputs(self, top_words:str, word_counts:str):pass

    @classmethod
    def creates_new_document(self):
        return True   

    def task_body(self):

        counter = Counter()
        for row in self.data.itertuples():
            
            stopwords = mc_providers.language.stopwords_for_language(row.language)
            words = [w for w in segwords(row.text.lower()) if w not in stopwords and len(w) > 1]
            
            counter += Counter(words)

        words, counts = zip(*sorted(counter.items(), key=lambda item: item[1], reverse=True)[:])

        self.results.top_words = words
        self.results.word_counts = counts




