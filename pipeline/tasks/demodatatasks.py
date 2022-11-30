from prefect import task
from ..flowatom import FlowAtom
from random import random
import pandas as pd
import numpy as np


class RandomSeriesTaskOutput():
    numbers:int

@FlowAtom.register("GenerateRandomSeriesTask")
class GenerateRandomSeriesTask(FlowAtom):
    
    sample_size:int
    _defaults:{
        "sample_size":10
    }
        
    def f_output(self):
        return RandomSeriesTaskOutput
    
    def task_body(self):

        df = pd.DataFrame(np.random.randint(0,100,size=(self.sample_size)), columns=["numbers"])
        self.write_data(df)
        

        
class PrimeFactorsTaskInput():
    to_factorize:int
        
class PrimeFactorsTaskOutput():
    factors:[int]

@FlowAtom.register("ListPrimeFactorsTask")
class ListPrimeFactorsTask(FlowAtom):
    
    def f_input(self):
        return PrimeFactorsTaskInput
        
    def f_output(self):
        return PrimeFactorsTaskOutput
            
    def task_body(self):

        data = self.get_data()
        output = []
        for num in data.to_factor:
            output.append(factorize(num))
        
        data.factors = output
        self.write_data(data)
            
        
class CountItemsInput():
    to_count:list

class CountItemsOutput():
    count:int
        
@FlowAtom.register("CountItemsTask")
class CountItems(FlowAtom):
    
    def f_input(self):
        return CountItemsInput
    
    def f_output(self):
        return CountItemsOutput
    
    def task_body(self):
        
        data = self.get_data()
        output = []
        for l in data.to_count:
            output.append(len(l))
        data.count = output
        self.write_data(data)

    
        
class PrintFieldInput():
    to_print:True
        
@FlowAtom.register("PrintFieldTask")
class PrintField(FlowAtom):
    
    pre_message:str
    post_message:str
    _defaults:{
        "pre_message":"Print Field Task Start",
        "post_message":"Print Field Task End"
    }
    
    def f_input(self):
        return PrintFieldInput
    
    def task_body(self):
        data = self.get_data()
        print("="*len(self.pre_message))
        print(self.pre_message)
        print(data.to_print)
        print(self.post_message)
        
        
#Really dumb method to factorize a number
@task()
def factorize(num):
    factors = []
    for i in range(2, num-1):
        if num % i == 0:
            factors.append(i)
    return factors
    