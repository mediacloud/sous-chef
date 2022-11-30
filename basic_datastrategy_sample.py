from pipeline import RunPipeline

#And of course eventually we can define these guys as yaml or whatever
config = {
    "name":"basic_datastrategy_sample",
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
            "id":"ListPrimeFactorsTask",
            "params":{
                "task_name":"factors"
            },
            "inputs":{
                "to_factor":"numbers"
            },
            "outputs":{
                "factors":"factors"
            }
        },
        {
            "id":"CountItemsTask",
            "params":{
                "task_name":"count"
            },
            "inputs":{
                "to_count":"factors"
            },
            "outputs":{
                "count":"factor_count"
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
            "id":"PrintFieldTask",
            "params":{
                "task_name":"print factors",
                "pre_message":"Printing Factors"
            }, 
            "inputs":{
                "to_print":"factors"
            }
        }
    ]
}


if __name__ == "__main__":
    RunPipeline(config)