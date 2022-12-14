from prefect import task
from ..flowatom import FlowAtom
from random import random
import pandas as pd
import numpy as np


@FlowAtom.register("GenerateRandomSeriesTask")
class GenerateRandomSeriesTask(FlowAtom):
    """
    Populate a row of 'sample_size' random values in between zero and the provided 'rand-range'
    A Demo Task for datastorage validation
    """
    sample_size:int
    rand_range:int
    _defaults:{
        "sample_size":10,
        "rand_range":100
    }
        
        
    @classmethod
    def creates_new_document(self):
        return True   
    
    def outputs(self, numbers:int): pass
    
    def task_body(self):
        self.results = pd.DataFrame(np.random.randint(0,self.rand_range,size=(self.sample_size)), columns=["numbers"])

    

@FlowAtom.register("ListPrimeFactorsTask")
class ListPrimeFactorsTask(FlowAtom):
    """
    Calculate the list of prime factors of an input number. 
    A Demo Task for datastorage validation
    """
    
    def inputs(self, to_factor:int): pass
    def outputs(self, factors:list): pass
    
    def task_body(self):
        output = []
        for num in self.data.to_factor:
            output.append(factorize(num))
        
        self.results.factors = output
        
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
    """
    Count the number of items stored in a list
    A Demo Task for datastorage validation
    """
    def inputs(self, to_count:list): pass
    def outputs(self, counted:int): pass
    
    def task_body(self):
        output = []
        for l in self.data.to_count:
            output.append(len(l))
        self.results.counted = output


        
@FlowAtom.register("PrintFieldTask")
class PrintField(FlowAtom):
    """
    Print a summary of the contents of a field to the console.
    A useful utility task. 
    """
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
    """
    Print a summary of the contents of a string field to the console.
    A useful utility task. 
    """
    def inputs(self, to_print:str): pass
            
            
@FlowAtom.register("DivisibleByNTask")
class DivisibleByNTask(FlowAtom):
    """
    Calculate whether or not a number is divisible by input parameter 'n'
    A Demo Task for datastorage validation
    """
    
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
        
        self.results.divisible = output
    
@FlowAtom.register("MostCommonTask")
class CommonElements(FlowAtom):
    """ take a field of lists and return the top-n most common elements """
    
    top_n:int
    _defaults:{
        "top_n":5
    }
        
    def inputs(self, to_count:list): pass
    def outputs(self, top_elements:int):pass
    
    @classmethod
    def creates_new_document(self):
        return True
    
    def task_body(self):
        elements = {}
        for doc in self.data.to_count:
            for item in doc:
                if item in elements:
                    elements[item] += 1
                else:
                    elements[item] = 1
        
        print(elements)
        top_elements = sorted(elements)
        if self.top_n > 0:
            top_elements = top_elements[:self.top_n]
        
        print(top_elements)
        self.results.top_elements = top_elements