from pipeline import RunPipeline

#And of course eventually we can define these guys as yaml or whatever
config = {
    "steps":[
    {
        "id":"TestParamAccessTask",
        "params":{
            "task_name":"the first one",
            "value":"ten"
        }
    },
    {
        "id": "PrintRandomValuesTask",
        "params":{
            "task_name": "the second one",
            "iterations":5
        }
    },
    {
        "id": "TestParamAccessTask",
        "params":{
            "task_name":"the third one",
            "value":"five"
        }
    },
    {
        "id": "PrintParamsWithDefaults",
        "params":{
            "a":25
            #b and c are set by defaults in the atom definition
        }
    },
    
]}


if __name__ == "__main__":
    RunPipeline(config)