import mcproviders as providers
from ..flowatom import FlowAtom
from datetime import datetime
import pandas as pd
'media_name', 'media_url', 'id', 'title', 'publish_date', 'url', 'last_updated', 'author', 'language', 'retweet_count', 'reply_count', 'like_count', 'quote_count', 'content'

@FlowAtom.register("SampleTwitter")
class sample_twitter(FlowAtom):
    
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
        results = SearchInterface.sample(self.query, start_date, end_date)
        print(len(results))
        
        #print(results[0].keys())
        self.data = pd.json_normalize(results)
        self.data["media_id"] = self.data["id"]
        
        