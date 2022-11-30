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
                "counted":"factor_count"
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
        },
        {
            "id":"PrintFieldTask",
            "params":{
                "task_name":"print number of factors",
                "pre_message":"Printing Number of Factors"
            }, 
            "inputs":{
                "to_print":"factor_count"
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
            "id":"DivisibleByNTask",
            "params":{
                "task_name":"is count divisible by five?",
                "n":5
            }, 
            "inputs":{
                "to_divide":"factor_count"
            },
            "outputs":{
                "divisible":"5_divisible"
            }
        },
        {
            "id":"PrintFieldTask",
            "params":{
                "task_name":"print divisiblity",
                "pre_message":"Printing divisibility"
            }, 
            "inputs":{
                "to_print":"5_divisible"
            }
        }
    ]
}


if __name__ == "__main__":
    RunPipeline(config)