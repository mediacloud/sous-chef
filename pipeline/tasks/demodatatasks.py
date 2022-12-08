from prefect import task
from ..flowatom import FlowAtom
from random import random
import pandas as pd
import numpy as np


@FlowAtom.register("GenerateRandomSeriesTask")
class GenerateRandomSeriesTask(FlowAtom):
    
    sample_size:int
    rand_range:int
    _defaults:{
        "sample_size":10,
        "rand_range":100
    }
        
    def outputs(self, numbers:int): pass
    
    def task_body(self):
        self.data = pd.DataFrame(np.random.randint(0,self.rand_range,size=(self.sample_size)), columns=["numbers"])

    

@FlowAtom.register("ListPrimeFactorsTask")
class ListPrimeFactorsTask(FlowAtom):

    def inputs(self, to_factor:int): pass
    def outputs(self, factors:list): pass
    
    def task_body(self):
        output = []
        for num in self.data.to_factor:
            output.append(factorize(num))
        
        self.data.factors = output
        
#Really dumb method to factorize a number
@task()
def factorize(num):
    factors = []
    for i in range(2, num-1):
        if num % i == 0:
            factors.append(i)
    return factors            

        
@FlowAtom.register("CountItemsTask")
class CountItems(FlowAtom):
    
    def inputs(self, to_count:list): pass
    def outputs(self, counted:int): pass
    
    def task_body(self):
        output = []
        for l in self.data.to_count:
            output.append(len(l))
        self.data.counted = output


        
@FlowAtom.register("PrintFieldTask")
class PrintField(FlowAtom):
    
    pre_message:str
    post_message:str
    _defaults:{
        "pre_message":"Print Field Task Start",
        "post_message":"Print Field Task End"
    }
    
    #Nonetype here specifies that we don't care about the input type, we'll work with anything
    def inputs(self, to_print:None): pass
    
    def task_body(self):
        print("="*len(self.pre_message))
        print(self.pre_message)
        print(self.data.to_print)
        print(self.post_message)
               

@FlowAtom.register("PrintStringTask")
class PrintString(PrintField):
    def inputs(self, to_print:str): pass
            
            
@FlowAtom.register("DivisibleByNTask")
class DivisibleByNTask(FlowAtom):
    
    n:int
    _defaults:{
        "n": 2
    }
            
    def inputs(self, to_divide:int): pass
    def outputs(self, divisible:bool): pass
    
    def task_body(self):
        output = []
        
        for val in self.data.to_divide:
            output.append(val % self.n == 0)
        
        self.data.divisible = output
    