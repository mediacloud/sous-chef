from pipeline import RunPipeline


config = {
    "name":"TwitterProcessingSample",
    "data_strategy":{
        "id":"PandasStrategy",
        "data_location":"data/"
    },
    "steps":[
        {
            "id":"QueryTwitter",
            "params":{
                "task_name":"Generate",
                "query":"#FIFAWorldCup Japan USA",
                "start_date":"2022-11-30",
                "end_date":"2022-12-01",         
            },
            "outputs":{
                'title':'title', 
                'publish_date':'publish_date', 
                'url':'url', 
                'author':'author', 
                'content':'content',
                'language':'tweet_language'
            }
        },
        {
            "id":"PrintStringTask",
            "params":{
                "task_name":"Print Tweets"
            },
            "inputs":{
                "to_print":"content"
            }
        },
    #    {
    #        "id":"TweetSentimentTask",
    #        "params":{
    #            "task_name":"Sentiment Task"
    #        },
    #        "inputs":{
    #            "tweets":"content"
    #        },
    #        "outputs":{
    #            "sentiment_label":"sentiment"
    #        }
    #    },
        {
            "id":"APIEntityExtraction",
            "params":{
                "task_name":"EntityExtraction"
            },
            "inputs":{
                "text":"content",
                "language":"tweet_language"
            },
            "outputs":{
                "entities":"entities"
            }
            
        }
    ]
}


if __name__ == "__main__":
    RunPipeline(config)