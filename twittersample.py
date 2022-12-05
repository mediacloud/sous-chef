from pipeline import RunPipeline

#OK a thing worth noting here is that
#WE DO NOT CARE about all of the outputs to the sampletwitter task.
#It should be the case that task inputs are compulsory, but task outputs are not!
config = {
    "name":"TwitterProcessingSample",
    "data_strategy":{
        "id":"PandasStrategy",
        "data_location":"data/"
    },
    "steps":[
        {
            "id":"SampleTwitter",
            "params":{
                "task_name":"Generate",
                "query":"#FIFAWorldCup",
                "start_date":"2022-11-30",
                "end_date":"2022-12-01"
            },
            "outputs":{
                'media_name':'media_name', 
                'media_url':'media_url', 
                'media_id':'media_id', 
                'title':'title', 
                'publish_date':'publish_date', 
                'url':'url', 
                'last_updated':'last_updated',
                'author':'author', 
                'language':'language', 
                'retweet_count':'retweet_count', 
                'reply_count':'reply_count', 
                'like_count':'like_count', 
                'quote_count':'quote_count', 
                'content':'content'
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
        }
    ]
}


if __name__ == "__main__":
    RunPipeline(config)