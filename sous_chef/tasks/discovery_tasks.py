from ..flowatom import FlowAtom
from ..exceptions import NoDiscoveryException
from datetime import date, datetime, timedelta
import pandas as pd
import requests
from requests.exceptions import ReadTimeout
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
    start_date:date
    end_date:date
    window_size:int
    api_key_block:str
    endpoint:str 
    timeout:int


    _defaults:{
        "api_key_block":"mediacloud-api-key",
        "date_mode":"direct",
        "start_date":"",
        "end_date":"",
        "window_size":"",
        "endpoint":"default",
        "timeout":600,
    }

    
    @classmethod
    def creates_new_document(self):
        return True
    
    def validate(self):
        pass
        """
        if self.date_mode=="direct":
            self.start_date_form = validate_datestr_form(self.start_date, "start_date")
            self.end_date_form = validate_datestr_form(self.end_date, "end_date")
                
            self.start_date = datetime.strptime(self.start_date, self.start_date_form)
            self.end_date = datetime.strptime(self.end_date, self.end_date_form)

        elif self.date_mode == "daily":
            self.end_date = datetime.today()
            self.start_date = datetime.today() - timedelta(days=self.window_size)
        """

def get_onlinenews_collection_domains(collection_ids, **kwargs):
    
    mc_api_key = Secret.load("mediacloud-api-key")
    directory = mediacloud.api.DirectoryApi(mc_api_key.get())
    
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
    sources:list
    _defaults:{
        "collections":[],
        "sources":[],
    }
        
    def validate(self):
        if "[" in self.collections:
            self.collections = ast.literal_eval(self.collections)
        if "[" in self.sources:
            self.sources = ast.literal_eval(self.sources)
    
    def outputs(self, title:str, language:str, media_name:str, url:str, 
                publish_date:object, text:str, id:str):pass
    
    def task_body(self):
        
        self.info(f"Query Text: {self.query}")
        self.info(f"Query Start Date: {self.start_date}, Query End Date: {self.end_date}")

        mc_api_key = Secret.load(self.api_key_block)
        mc_search = mediacloud.api.SearchApi(mc_api_key.get())
        mc_search.TIMEOUT_SECS = self.timeout
        if self.endpoint != "default":   
            mc_search.BASE_API_URL = self.endpoint

        all_stories = []
        pagination_token = None
        more_stories = True
        start_time = time.time()
        while more_stories:
            page, pagination_token = mc_search.story_list(self.query, start_date=self.start_date, 
                                                        end_date=self.end_date, collection_ids=self.collections,
                                                        source_ids=self.sources, pagination_token=pagination_token, 
                                                        expanded=True)
            all_stories += page
            more_stories = pagination_token is not None

        end_time = time.time()
        elapsed_time = end_time - start_time

        content = []
        for article in all_stories:
            if "text" in article:
                content.append(article)
        
        self.info(f"Query Returned {len(content)} Articles")
        

        if len(content) > 0:
            self.info(content[0])
            self.results = pd.json_normalize(content)
        else:
            print(content)
            raise NoDiscoveryException(f"Query {self.query} produced no content")


        self.return_values["QueryOverview"] = f"Query Text: {self.query}, Query Start Date: {self.start_date}, Query End Date: {self.end_date}"
        self.return_values["DocumentCount"] = len(content)
        self.return_values["ElapsedTime"] = round(elapsed_time, 1)


@FlowAtom.register("CountOverTime")
class onlinenews_count_over_time(DiscoveryAtom):
    collections:list
    _defaults:{
        "collections":[],
    }

    def validate(self):
        if "[" in self.collections:
            self.collections = ast.literal_eval(self.collections)
    
    def outputs(self, date:str, total_count:int, ratio:float): pass

    def task_body(self):
        
        self.info(f"Query Text: {self.query}")
        self.info(f"Query Start Date: {self.start_date}, Query End Date: {self.end_date}")

        mc_api_key = Secret.load(self.api_key_block)
        mc_search = mediacloud.api.SearchApi(mc_api_key.get())
        mc_search.TIMEOUT_SECS = self.timeout
        if self.endpoint != "default":   
            mc_search.BASE_API_URL = self.endpoint

        start_time = time.time()
        try:
            count_over_time = mc_search.story_count_over_time(self.query, 
                                                    start_date=self.start_date, 
                                                    end_date=self.end_date, 
                                                    collection_ids=self.collections)
        except ReadTimeout or RuntimeError as e:
            self.warn(e)
            self.warn("continuing...")
            elapsed_time = 0 
        else:

            end_time = time.time()
            elapsed_time = end_time - start_time
            
            self.results = pd.DataFrame(count_over_time)


        self.return_values["ElapsedTime"] = elapsed_time


            
            
            
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
            
        
        
