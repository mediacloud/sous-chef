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
                "query":"#FIFAWorldCup Netherlands USA",
                "start_date":"2022-12-03, 12:30",
                "end_date":"2022-12-04",         
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
        {
            "id":"TweetSentimentTask",
            "params":{
                "task_name":"Sentiment Task"
            },
            "inputs":{
                "tweets":"content"
            },
            "outputs":{
                "sentiment_label":"sentiment"
            }
        },
        {
            "id":"OutputCSV",
            "params":{
                "task_name":"EntityExtraction",
                "columns":["publish_date", "content", "sentiment"],
                "output_location":"~/twitter_sample.csv"
            }  
        },
        {
            "id":"OutputFieldHistogram",
            "params":{
                "columns":["sentiment"],
                "output_location":"./twitter_sample_sentiment.jpg"
            }
        },
        {
            "id":"OutputTimeSeriesHistogram",
            "params":{
                "columns":["publish_date", "sentiment"],
                "date_index_column":"publish_date",
                "values_column":"sentiment",
                "output_location":"./twitter_series_sentiment.jpg"
            }
        }
    ]
}


if __name__ == "__main__":
    RunPipeline(config)