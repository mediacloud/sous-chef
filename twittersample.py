from pipeline import RunPipeline


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
                'title':'title', 
                'publish_date':'publish_date', 
                'url':'url', 
                'author':'author', 
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