from ..flowatom import FlowAtom
from datetime import datetime
import pandas as pd
import requests
from .utils import lazy_import
from waybacknews.searchapi import SearchApiClient as WaybackSearchClient
import re

from pprint import pprint

#import mcproviders as providers
providers = lazy_import("mcproviders")

#A helper function to apply to datestring config inputs
def validate_datestr_form(datestr, name):
    time_formats = ['%Y-%m-%d', '%Y-%m-%d, %H:%M', "%Y-%m-%d, %H:%M %p"]
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
    start_date:str
    end_date:str
    
    @classmethod
    def creates_new_document(self):
        return True
    
    def validate(self):
        self.start_date_form = validate_datestr_form(self.start_date, "start_date")
        self.end_date_form = validate_datestr_form(self.end_date, "end_date")
        
        
@FlowAtom.register("SampleTwitter")
class sample_twitter(DiscoveryAtom):
    """ 
    Get a small sample of tweets matching a query using the mc-providers package
    """

    max_results:int
    _defaults:{
        "max_results":100
    }
        
    def outputs(self, media_name:str, media_url:str, media_id:int, title:str, 
                publish_date:object, url:str, last_updated:object, author:str, language:str, 
                retweet_count:int, reply_count:int, like_count:int, quote_count:int, 
                content:str): pass
        
    def task_body(self):
        SearchInterface = providers.provider_by_name("twitter-twitter")
        start_date = datetime.strptime(self.start_date, self.start_date_form)
        end_date = datetime.strptime(self.end_date, self.end_date_form)
        
        results = SearchInterface.sample(self.query, start_date, end_date, limit=self.max_results)
        
        self.results = pd.json_normalize(results)
        
        self.results["media_id"] = self.results["id"]
        
        
@FlowAtom.register("QueryTwitter")
class query_twitter(DiscoveryAtom):
    """ 
    Get all tweets matching a query using the mc-providers package. 
    """

    def outputs(self, media_name:str, media_url:str, media_id:int, title:str, 
                publish_date:object, url:str, last_updated:object, author:str, language:str, 
                retweet_count:int, reply_count:int, like_count:int, quote_count:int, 
                content:str): pass

    def task_body(self):
        SearchInterface = providers.provider_by_name("twitter-twitter")
        start_date = datetime.strptime(self.start_date, self.start_date_form)
        end_date = datetime.strptime(self.end_date, self.end_date_form)
        
        output = []
        for result in SearchInterface.all_items(self.query, start_date, end_date):
            output.extend(result)
            
        self.results = pd.json_normalize(output)
        self.results["media_id"] = self.results["id"]
        
        
        
@FlowAtom.register("QueryOnlineNews")
class query_onlinenews(DiscoveryAtom):
    

    def outputs(self, title:str, language:str, domain:str, original_capture_url:str, 
                publication_date:object, text:str):pass
        
    
    def task_body(self):
        provider = "onlinenews-waybackmachine"
        
        SearchInterface = WaybackSearchClient("mediacloud")

        
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        output = []
        for result in SearchInterface.all_articles(self.query, start_date, end_date):
            output.extend(result)

        #print(output)
        
        content = []
        for article in output:
            article_url = article["article_url"]
            article_info = requests.get(article["article_url"]).json()
            if "snippet" in article_info:
                print("Got Article")
                content.append(article_info)
        
        if len(content) > 0:
            self.results = pd.json_normalize(content)
            self.results["text"] = self.results["snippet"]
