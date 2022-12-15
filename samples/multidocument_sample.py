from pipeline import RunPipeline

#And of course eventually we can define these guys as yaml or whatever
config = {
    "name":"multidocument_sample",
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
                "task_name":"Count Factors"
            },
            "inputs":{
                "to_count":"factors"
            },
            "outputs":{
                "counted":"factor_count"
            }
        },
        {
            "id":"MostCommonTask",
            "params":{
                "task_name":"Most Common Factors",
                "top_n":10
            },
            "inputs":{
                "to_count":"factors"
            },
            "outputs":{
                "top_elements":"top_factors"
            }
        },
        {
            "id":"OutputCSV",
            "params":{
                "task_name":"EntityExtraction",
                "columns":["factors", "factor_count"],
                "output_location":"~/simple_exporter.csv"
            }  
        },
        {
            "id":"DivisibleByNTask",
            "params":{
                "task_name":"idk man",
                "n":2
            },
            "inputs":{
                "to_divide":"top_factors"
            },
            "outputs":{
                "divisible":"divisible"
            }
        },
        
    ]
}


if __name__ == "__main__":
    RunPipeline(config)