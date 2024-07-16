from ..flowatom import FlowAtom
from ..exceptions import NoDiscoveryException
from datetime import datetime, timedelta
import pandas as pd
import requests
from .utils import lazy_import
from waybacknews.searchapi import SearchApiClient as WaybackSearchClient
from prefect.blocks.system import Secret
import mediacloud.api
import time
import re
import os
import ast

from pprint import pprint

#import mcproviders as providers
providers = lazy_import("mc_providers")
mcmetadata = lazy_import("mcmetadata")


#A helper function to apply to datestring config inputs
def validate_datestr_form(datestr, name):
    time_formats = ['%Y-%m-%d', '%Y-%m-%d, %H:%M', "%Y-%m-%d, %H:%M %p", 
                    '%m-%d-%Y', '%m-%d-%Y, %H:%M', "%m-%d-%Y, %H:%M %p"]
    good_form = None
    for form in time_formats:
        try:
            datetime.strptime(datestr, form)
            good_form = form
        except ValueError:
            continue
            
    if good_form == None:
        raise RuntimeError("Validation Error- invalid datetime provided")
    else:
        return good_form

#Maybe carrage returns fuck it up... 
#Can add other preprocessing here as needed
def clean_text(text):
    return re.sub("\\n|\\r", " ", text)



#All the discovery atoms impliment the same validation, so we should be able to just subclass this. 
class DiscoveryAtom(FlowAtom):
    query:str
    date_mode:str
    start_date:str
    end_date:str
    window_size:int


    _defaults:{
        "date_mode":"direct",
        "start_date":"",
        "end_date":"",
        "window_size":""
    }

    
    @classmethod
    def creates_new_document(self):
        return True
    
    def validate(self):
        if self.date_mode=="direct":
            self.start_date_form = validate_datestr_form(self.start_date, "start_date")
            self.end_date_form = validate_datestr_form(self.end_date, "end_date")
                
            self.start_date = datetime.strptime(self.start_date, self.start_date_form)
            self.end_date = datetime.strptime(self.end_date, self.end_date_form)

        elif self.date_mode == "daily":
            self.end_date = datetime.today()
            self.start_date = datetime.today() - timedelta(days=self.window_size)
        

def get_onlinenews_collection_domains(collection_ids, **kwargs):
    
    mc_api = Secret.load("mediacloud-api-key")
    directory = mediacloud.api.DirectoryApi(mc_api.get())
    
    domains = []
    for collection in collection_ids:
        
        sources = directory.source_list(collection_id=collection, limit=100000,  **kwargs)
        
        for res in sources["results"]:
            if "/" not in res["name"]:
                domains.append(res["name"])
            
       
    return domains

        
@FlowAtom.register("QueryOnlineNews")
class query_onlinenews(DiscoveryAtom):
    
    """
    Query mediacloud's onlinenews collection
    """
    
    collections:list
    _defaults:{
        "collections":[]
    }
        
    def validate(self):
        if "[" in self.collections:
            self.collections = ast.literal_eval(self.collections)
    
    def outputs(self, title:str, language:str, domain:str, original_capture_url:str, 
                publication_date:object, text:str, id:str):pass
    
    
    def task_body(self):
        
        self.info(f"Query Text: {self.query}")
        self.info(f"Query Start Date: {self.start_date}, Query End Date: {self.end_date}")
        
        api_key = Secret.load("mediacloud-api-key")
        mc_search = mediacloud.api.SearchApi(api_key.get())
        all_stories = []
        pagination_token = None
        more_stories = True
        start_time = time.time()
        while more_stories:
            page, pagination_token = mc_search.story_list(self.query, start_date=self.start_date.date(), 
                                                        end_date=self.end_date.date(), collection_ids=self.collections,
                                                        pagination_token=pagination_token, expanded=True)
            all_stories += page
            more_stories = pagination_token is not None

        end_time = time.time()
        elapsed_time = end_time - start_time

        content = []
        for article in all_stories:
            if "text" in article:
                content.append(article)
        
        self.info(f"Query Returned {len(content)} Articles")
        self.info(content[0])

        if len(content) > 0:
            self.results = pd.json_normalize(content)
        else:
            print(content)
            raise NoDiscoveryException(f"Query {self.query} produced no content")

        
        self.return_values["QueryOverview"] = f"Query Text: {self.query}, Query Start Date: {self.start_date}, Query End Date: {self.end_date}"
        self.return_values["QueryCount"] = f"Query Returned {len(content)} Articles"
        self.return_values["ElapsedTime"] = elapsed_time

            
@FlowAtom.register("CountOnlineNews")
class count_onlinenews(DiscoveryAtom):
    """
    Gets the count as returned from onlinenews.count(query)
    """
    
    collections:list
    _defaults:{
        "collections":[]
    }
        
    def outputs(self, count:int):pass
    
    def validate(self):
        if "[" in self.collections:
            self.collections = ast.literal_eval(self.collections)
            
    
    def task_body(self):
        provider = "onlinenews-mediacloud"
        base_url = "http://ramos.angwin:8000/v1/" 
        
        SearchInterface = providers.provider_by_name(provider, None, base_url)
        
        start_date = datetime.strptime(self.start_date, self.start_date_form)
        end_date = datetime.strptime(self.end_date, self.end_date_form)
    
        domains = []
        if len(self.collections) > 0:
            domains = get_onlinenews_collection_domains(self.collections)
            
        count = SearchInterface.count(self.query, start_date, end_date, domains = domains)
        self.results = pd.DataFrame()
        self.results["count"] = [count]
            
            
            
@FlowAtom.register("GetWebpageContent")
class get_web_metadata(FlowAtom):
    """
    Use Mc-metadata to extract metadata content from a list of urls
    """
    
    urls: list
    _defaults:{
        "urls":[]
    }
        
    @classmethod
    def creates_new_document(self):
        return True
        
    def outputs(self, url:str, article_title: str, text_content:str, language:str): pass
    
    def task_body(self):
        
        content = []
        for url in self.urls:
            
            result = mcmetadata.extract(url)
            content.append(result)
        
        if len(content) > 0:
            self.results = pd.json_normalize(content)
            
        
        
