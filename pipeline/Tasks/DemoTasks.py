from prefect import task
from ..flowatom import FlowAtom
from random import random


#It is good to define steps of the atoms as tasks
#so that we can take advantage of things like retries
#and other prefect magic 
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
    

@FlowAtom.register("TestParamAccessTask")
class PrintParameters(FlowAtom):
    
    value:str
    
    def task_body(self):
        task_with_input(self.value)
        
        
@FlowAtom.register("PrintRandomValuesTask")
class PrintRandomValues(FlowAtom):
    
    iterations:int
    
    def task_body(self):
        for i in range(self.iterations):
            task_with_no_input()
            
@FlowAtom.register("PrintParamsWithDefaults")
class DefaultParameters(FlowAtom):
    
    a:int
    b:int
    c:int
        
    _defaults:{
        "a":1,
        "b":2,
        "c":3,
    }
        
    def task_body(self):
        task_with_three_inputs(self.a, self.b, self.c)