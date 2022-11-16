from prefect import flow, task
from random import random

@task()
def start_task(val):
    print(f"start {val}")
    
@task()
def end_task(val):
    print(f"end {val}")

    
@task()
def task_with_input(in_):
    print(in_)
    
@task()
def task_with_no_input():
    print(random())
    
@task()
def task_with_three_inputs(a, b, c):
    print(f"a:{a}, b:{b*2}, c:{c}")

    
class FlowAtom(object):
    _REGISTERED_ATOMS = {}
    
    def __init__(self, params):
        self.params = params
    
    
    #This takes care of registering the atom locally for our pipeline project,
    #and also running the prefect registration hook. 
    #Eventually might need to break out more of rest of the prefect functionality but leave as is for now
    @classmethod
    def register(cls, name):
        def _register(stepclass):
            cls._REGISTERED_ATOMS[name] = flow(stepclass, name=name)
            return stepclass 
        return _register
    
    def task_body(self):
        raise RunTimeError("task_body is Unimplimented")
    
    def __pre_task(self):
        #These might not be needed in a real deployment but i'm just demonstrating what we can do
        start_task(self.params["name"])
        
    def __post_task(self):
        end_task(self.params["name"])
    
    def __call__(self):
        self.__pre_task()
        self.task_body()
        self.__post_task()
    

@FlowAtom.register("TestParamAccessTask")
class TaskOne(FlowAtom):
    def task_body(self):
        task_with_input(self.params["value"])
        task_with_three_inputs(1,2,3)
        

@FlowAtom.register("PrintRandomValuesTask")
class TaskTwo(FlowAtom):
    def task_body(self):
        for i in range(self.params["iterations"]):
            task_with_no_input()

        
class Pipeline():
    def __init__(self, config):
        self.config = config
    
    def __validate(self):
        
    
    def do_flow(self):
        for task in self.config:
            FlowAtom._REGISTERED_ATOMS[task["id"]](task["params"])()

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

#This, then is all we really need
@flow()
def compose_pipeline(config):
    pl = Pipeline(config)
    pl.do_flow()
    
compose_pipeline(config)