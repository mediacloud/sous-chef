from prefect import flow, task
from random import random

"""
This a living scratch work document to work out the abstraction needed to have a little configurable prefect flow system.

"""

@task()
def test_task_1(name):
    print(name)
    return

@task()
def test_task_2(name):
    print(name * 2)
    return

@task()
def test_task_3(name):
    print(random())
    return

@task()
def start_task():
    print("this task runs first")
    return
    
    
@task()
def end_task():
    print("this task runs last")
    return


@flow()
def test_flow1(name):
    start_task()
    
    test_task_1(name)

    end_task()
    
    
@flow()
def test_flow2(name):
    start_task()
    
    test_task_2(name)
    
    end_task()
    
@flow()
def test_flow3(name):
    start_task()
    
    test_task_3(name)
    
    end_task()
    
flow_atoms = {
    "one":test_flow1,
    "two":test_flow2,
    "three":test_flow3
    
}
   
    
class FlowAuthor():
    #This is the glue- it will eventually be responsible for all sorts of 
    #validation of config setups and such, I assume. Feels overkill without that functionality though
    def __init__(self, name, config):
        self.name = name
        self.config = config
        
    
    def do_flow(self):
        for task in self.config:
            flow_atoms[task](self.name)

        

@flow()
def composite_flow(name, config):
    author = FlowAuthor(name, config)
    author.do_flow()
        
config = ["one", "two", "two", "three", "one", "three", "three"]

#This is cool! Some problems:
#Atom inputs- there is some work to make them nicely configurable with settings and everything.
#Wrapping the atoms up as objects that can do their own validation requires figuring out how to 
#actually instantiate the flow as an object function. 
#This situation happening with composite_flow, where it's like a combinator function, I guess could maybe be the way?
#But it feels tacky.
#Let's have a take two

composite_flow("name", config)

