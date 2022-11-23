from prefect import task
from FlowAtom import FlowAtom
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
    

@FlowAtom.register("TestParamAccessTask")
class PrintParameters(FlowAtom):
    def task_body(self):
        task_with_input(self.params["value"])
        task_with_three_inputs(1,2,3)
        

@FlowAtom.register("PrintRandomValuesTask")
class PrintRandomValues(FlowAtom):
    def task_body(self):
        for i in range(self.params["iterations"]):
            task_with_no_input()