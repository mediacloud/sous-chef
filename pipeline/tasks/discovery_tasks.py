from ..flowatom import FlowAtom
from datetime import datetime
import pandas as pd

from .utils import lazy_import
#import mcproviders as providers

providers = lazy_import("mcproviders")

@FlowAtom.register("SampleTwitter")
class sample_twitter(FlowAtom):
    """ 
    Get a small sample of tweets matching a query using the mc-providers package
    """
    
    query:str
    start_date:str
    end_date:str
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
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        results = SearchInterface.sample(self.query, start_date, end_date, limit=self.max_results)
        
        self.data = pd.json_normalize(results)
        self.data["media_id"] = self.data["id"]
        
        
@FlowAtom.register("QueryTwitter")
class query_twitter(FlowAtom):
    """ 
    Get all tweets matching a query using the mc-providers package. 
    """
    query:str
    start_date:str
    end_date:str
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
        start_date = datetime.strptime(self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(self.end_date, '%Y-%m-%d')
        
        output = []
        for result in SearchInterface.all_items(self.query, start_date, end_date, limit=self.max_results):
            output.extend(result)
            
        self.data = pd.json_normalize(output)
        self.data["media_id"] = self.data["id"]
        
        
        


        