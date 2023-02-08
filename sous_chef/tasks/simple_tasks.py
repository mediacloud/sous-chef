from prefect import task
from ..flowatom import FlowAtom
from random import random


@FlowAtom.register("TestParamAccessTask")
class PrintParameters(FlowAtom):
    """
    Print the value of the provided parameter to the console
    A Demo Task for testing atom validation
    """
    value:str
    
    def task_body(self):
        task_with_input(self.value)
        
        
@FlowAtom.register("PrintRandomValuesTask")
class PrintRandomValues(FlowAtom):
    """
    Print a random value to the console the number of times specified
    A Demo Task for testing atom validation
    """
    iterations:int
    
    def task_body(self):
        for i in range(self.iterations):
            task_with_no_input()

            
@FlowAtom.register("PrintParamsWithDefaults")
class DefaultParameters(FlowAtom):
    """
    Print the three provided values to the console. 
    A default value will be provided if none if given at config time. 
    A Demo Task for testing atom validation
    """
    a:int
    b:int
    c:int
        
    #These values are used if nothing is provided in the configuration file
    #If no default is defined and a parameter is missing, an error is raised
    _defaults:{
        "a":1,
        "b":2,
        "c":3,
    }
        
    def task_body(self):
        task_with_three_inputs(self.a, self.b, self.c)


    
@task()
def task_with_input(in_):
    print(in_)
    
@task()
def task_with_no_input():
    print(random())
    
@task()
def task_with_three_inputs(a, b, c):
    print(f"a:{a}, b:{b}, c:{c}")
