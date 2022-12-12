from pipeline import RunPipeline


config = {
    "name":"OnlineNewsProcessingSample",
    "data_strategy":{
        "id":"PandasStrategy",
        "data_location":"data/"
    },
    "steps":[
        {
            "id":"QueryOnlineNews",
            "params":{
                "task_name":"Generate",
                "query":"covid AND vaccine AND fauci",
                "start_date":"2022-07-01",
                "end_date":"2022-07-02",         
            },
            "outputs":{
                "text":"text",
                "title":"title",
                "language":"language",
                "publication_date":"publication_date"
            }
        },
        {
            "id":"APIEntityExtraction",
            "params":{
                "task_name":"Extract"
            },
            "inputs":{
                "text":"text",
                "language":"language"
            },
            "outputs":{
                "entities":"entities"
            }
        },
        {
            "id":"TopNEntities",
            "params":{
                "task_name":"Count Entities",
                "top_n":100,
            },
            "inputs":{
                "entities":"entities"
            },
            "output":{
                "top_entities":"top_entities",
            }
        }
    ]
}


if __name__ == "__main__":
    RunPipeline(config)