from pipeline import RunPipeline

#And of course eventually we can define these guys as yaml or whatever
config = {
    "name":"filter_sample",
    "data_strategy":{
        "id":"PandasStrategy",
        "data_location":"data/"
    },
    "steps":[
        {
            "id":"GenerateRandomSeriesTask",
            "params":{
                "task_name":"Generate",
                "sample_size":20
            },
            "outputs":{
                "numbers":"numbers"
            }
        },
        {
            "id":"PrintFieldTask",
            "params":{
                "task_name":"print numbers",
                "pre_message":"Printing Original Numbers"
            }, 
            "inputs":{
                "to_print":"numbers"
            }
        },
        {
            "id":"FilterBelowN",
            "params":{
                "task_name":"filter this stuff",
                "n":50
            },
            "inputs":{
                "to_compare":"numbers"
            }
        },
                {
            "id":"PrintFieldTask",
            "params":{
                "task_name":"print numbers",
                "pre_message":"Printing Numbers Post Filter"
            }, 
            "inputs":{
                "to_print":"numbers"
            }
        },
        
    ]
}


if __name__ == "__main__":
    RunPipeline(config)