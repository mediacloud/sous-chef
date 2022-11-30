import pandas as pd
import numpy as np
import inspect
import os
import ast

from .constants import (ID, STEPS, DATA, DATASTRATEGY, DATALOCATION, READLOCATION, 
    WRITELOCATION, INPUTS, OUTPUTS, PARAMS, RUNNAME, NOSTRAT)


#There should be one data strategy per pipeline
#Each impliments three methods:

class DataStrategy(object):
    _REGISTERED_STRATS = {}
    
    @classmethod
    def get_strats(cls):
        return cls._REGISTERED_STRATS
    
    #This lets us register new data strats just like with the pipeline atoms
    @classmethod
    def register(cls, _name):
        
        def _register(stratclass):
            cls._REGISTERED_STRATS[_name] = stratclass
            return stratclass 
        return _register
    
    def __init__(self, config, _input, _output):
        self._function_inputs = inspect.get_annotations(_input)
        self._function_outputs = inspect.get_annotations(_output)
        
        if config is not None:
            for key, value in config.items():
                setattr(self, key, value)

        self.config = config
    
    # a class method update_config which reads a pipeline configuration 
    #and backfills each step with the information the strategy will need at each step
    @classmethod
    def setup_config(cls, config):
        pass
    
    #A generator which yeilds data from inputlocation
    def get_data(self):
        pass  
    
    #A method which writes data to outputlocation
    def write_data(self, data):
        pass
    
@DataStrategy.register(NOSTRAT)
class NoStrategy(DataStrategy):
        
    
    @classmethod
    def update_config(self, config):
        for i, step in enumerate(config[STEPS]):
            step[DATA] = None
            
        return config
    
    def get_data(self):
        return None
    
    def write_data(self, data):
        return None
    

#In this strategy, each configuration step will have to define where it wants to load and write data
#Maybe we'll have some naming convention- like taskname_field- 
#so the config just has to say something like input_cols:[a,b], output_cols:[d]. 
@DataStrategy.register("PandasStrategy")
class PandasStrategy(DataStrategy):
    
    @classmethod
    def setup_config(self, config):
        data_location_root = f"{config[DATASTRATEGY][DATALOCATION]}{config[RUNNAME]}"
        
        #iterate so we don't overwrite old runs unintentionally
        i = 0
        while os.path.exists(f"{data_location_root}_{i}.csv"):
            i += 1
        
        data_location = f"{data_location_root}_{i}.csv"
        
        start_dataframe = pd.DataFrame()
        start_dataframe.to_csv(data_location)
        
        for i, step in enumerate(config[STEPS]):
            
            #We could have some method of inferring inputs and outputs, if none are given
            #and also maybe we should have some method of making sure the input/outputs are unique.
            #but for now we just go with this. 
            data_meta = {
                DATASTRATEGY: config[DATASTRATEGY][ID],
                DATALOCATION: data_location,
                INPUTS: step[INPUTS] if INPUTS in step else None,
                OUTPUTS: step[OUTPUTS] if OUTPUTS in step else None 
            }
            
            step[DATA] = data_meta
                
            
        return config
    
    def get_data(self):
        read_dataframe = pd.read_csv(self.data_location)
        operating_dataframe = pd.DataFrame()
        
        for function_name, read_location in self.inputs.items():
            #apply literal eval so that types are preserved
            #read_dataframe[read_location].apply(lambda x:print(str(x)))
            typed =  read_dataframe[read_location].apply(lambda x:eval_or_nan(x))
            operating_dataframe[function_name] = typed
        
        if self.outputs is not None:
            #This just sets up the outputs so that the flow object can write to it with dot syntax
            for function_name, read_location in self.outputs.items():
                operating_dataframe[function_name] = pd.Series(dtype=object)
       
        return operating_dataframe

    def write_data(self, operating_dataframe):
        if self.outputs is not None:

            write_dataframe = pd.read_csv(self.data_location)
            for function_name, write_location in self.outputs.items():
                write_dataframe[write_location] = operating_dataframe[function_name]
            write_dataframe.to_csv(self.data_location)
        
        else:
            raise RuntimeError("Can't call write_data on an atom with no outputs defined")
        
    
def eval_or_nan(val):
    if str(val) == "nan":
        return val
    else:
        return ast.literal_eval(str(val))
