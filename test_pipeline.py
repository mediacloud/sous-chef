from pipeline import RunPipeline

#And of course eventually we can define these guys as yaml or whatever
config = [
    {
        "id":"TestParamAccessTask",
        "params":{
            "name":"the first one",
            "value":"ten"
        }
    },
    {
        "id": "PrintRandomValuesTask",
        "params":{
            "name": "the second one",
            "iterations":5
        }
    },
    {
        "id": "TestParamAccessTask",
        "params":{
            "name":"the third one",
            "value":"five"
        }
    },
    {
        "id": "PrintRandomValuesTask",
        "params":{
            "name": "do random values a second time",
            "iterations":2
        }
    },
    
]


if __name__ == "__main__":
    RunPipeline(config)