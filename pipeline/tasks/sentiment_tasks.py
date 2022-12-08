import numpy as np
#from .utils import lazy_import
from ..flowatom import FlowAtom
import os
import requests

from transformers import pipeline as hf_pipeline


HF_API_BEARER_TOKEN = os.getenv('HUGGINGFACE_API_BEARER_TOKEN', None)

#Recomended step for demangling twitter links and usernames
def preprocess_tweet(text):
    new_text = []
    for t in text.split(" "):
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

def apply_pipeline(pipeline, tweet):
    text = preprocess(tweet)
    return pipeline(text)
    
    
@FlowAtom.register("TweetSentimentTask")
class TweetSentimentTask(FlowAtom):
    """
    Use a local instance of a huggingface transformer to calculate sentiment values and scores on tweets
    """
    
    hf_model_name:str
        
    _defaults:{
        "hf_model_name":"cardiffnlp/twitter-roberta-base-sentiment-latest"
    }
        
    def inputs(self, tweets:str):pass
    def outputs(self, sentiment_label:str, sentiment_score:float):pass
    
    def task_body(self):
        #pipelines within pipelines
        
        
      
        sentiment_task = hf_pipeline("sentiment-analysis", 
                                     model=self.hf_model_name, 
                                     tokenizer=self.hf_model_name)
        
        model_response = []
        #Do this sequentially because logging
        for tweet in self.data.tweets:
            clean_tweet = preprocess_tweet(tweet)
            result = sentiment_task(clean_tweet)
            model_response.append(result)
            
        top_sentiment = [max(resp, key=lambda x: x['score']) for resp in model_response]
                  
        self.data.sentiment_label = [x["label"] for x in top_sentiment]
        self.data.sentiment_score = [x["score"] for x in top_sentiment]
        
        
#This just uses the huggingface api!
#A thing we might want to consider in the longterm, I suppose, 
#so we don't have to manage local MLOps
@FlowAtom.register("APITweetSentimentTask")
class ApiTweetSentimentTask(FlowAtom):
    """
    Use a huggingface API endpoint to apply a transformer to calculate sentiment values and scores on tweets
    """
    hf_model_name:str
        
    _defaults:{
        "hf_model_name":"cardiffnlp/twitter-roberta-base-sentiment-latest"
    }
    
    def inputs(self, tweets:str):pass
    def outputs(self, sentiment_label:str, sentiment_score:float):pass
    
    def task_body(self):
        
        API_URL = "https://api-inference.huggingface.co/models/" + self.hf_model_name
        headers = {"Authorization": "Bearer %s" % (HF_API_BEARER_TOKEN)}
        
        cleaned_tweets = [preprocess_tweet(t) for t in self.data.tweets]
        
        payload = dict(inputs=cleaned_tweets, options=dict(wait_for_model=True))
        model_response = requests.post(API_URL, headers=headers, json=payload).json()
        
        #model returns scores for all three possible labels, so let's just get the top one
        top_sentiment = [max(resp, key=lambda x: x['score']) for resp in model_response]
        
        self.data.sentiment_label = [x["label"] for x in top_sentiment]
        self.data.sentiment_score = [x["score"] for x in top_sentiment]
        
        