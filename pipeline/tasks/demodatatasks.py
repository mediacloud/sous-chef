from prefect import task
from ..flowatom import FlowAtom
from random import random
import pandas as pd
import numpy as np


@FlowAtom.register("GenerateRandomSeriesTask")
class GenerateRandomSeriesTask(FlowAtom):
    
    sample_size:int
    _defaults:{
        "sample_size":10
    }
    
    def task_body(self):

        df = pd.DataFrame(np.random.randint(0,100,size=(self.sample_size)), columns=["input"])
        self.write_data(df)
        


@FlowAtom.register("ListPrimeFactorsTask")
class ListPrimeFactorsTask(FlowAtom):
    
    def task_body(self):

        data = self.get_data()
        output = []
        for num in data["input"]:
            output.append(factorize(num))
        
        data["ListPrimes"] = output
        self.write_data(data)
            
        

#Really dumb method to factorize a number
@task()
def factorize(num):
    factors = []
    for i in range(2, num-1):
        if num % i == 0:
            factors.append(i)
    return factors
    