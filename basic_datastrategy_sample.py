from pipeline import RunPipeline

#And of course eventually we can define these guys as yaml or whatever
config = {
    "data_strategy":{
        "id":"CSVStrategy",
        "data_location":"data/"
    },
    "steps":[
        {
            "id":"GenerateRandomSeriesTask",
            "params":{
                "task_name":"Generate",
                "sample_size":20
            }
        },
        {
            "id":"ListPrimeFactorsTask",
            "params":{
                "task_name":"factors"
            }
        }
    ]
}


if __name__ == "__main__":
    RunPipeline(config)