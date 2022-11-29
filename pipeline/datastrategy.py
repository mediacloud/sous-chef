import pandas as pd

ID = "id"
STEPS = "steps"
DATA = "_data"
DATASTRATEGY = "data_strategy"
DATALOCATION = "data_location"
READLOCATION = "read_location"
WRITELOCATION = "write_location"
PARAMS = "params"

NOSTRAT = "NoStrategy"

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
    
    def __init__(self, config):
        if config is not None:
            self.data_location = config[DATALOCATION]
            self.read_location = config[READLOCATION]
            self.write_location = config[WRITELOCATION]
            self.config = config
    
    # a class method update_config which reads a pipeline configuration 
    #and backfills each step with the information the strategy will need at each step
    @classmethod
    def process_config(cls, config):
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
    
#This strategy writes the output of each step to its own file using pandas,
#and offers access to the previous step via get_data. 
#This is kinda bad- as it leaves the actual access to the dataframe to each
#flow atom implimentation, which is bad encapsulation. Iterate by leveraging pandas a bit more explicitly. 
@DataStrategy.register("CSVStrategy")
class CSVStrategy(DataStrategy):

    @classmethod
    def update_config(self, config):
        data_locations = []
 
        for i, step in enumerate(config[STEPS]):
            step_out = f"{i}_write.csv"
            data_locations.append(step_out)
            if i > 0:
                step_in = data_locations[-2]
            else:
                step_in = None
            
            data_meta = {
                DATASTRATEGY: config[DATASTRATEGY][ID],
                DATALOCATION: config[DATASTRATEGY][DATALOCATION],
                READLOCATION: step_in,
                WRITELOCATION: step_out
            }
            
            step[DATA] = data_meta
                
            
        return config
          
    
    def get_data(self):
        dataframe = pd.read_csv(f"{self.data_location}/{self.read_location}")
        return dataframe
    
    def write_data(self, dataframe):
        dataframe.to_csv(f"{self.data_location}/{self.write_location}")
        return True

@DataStrategy.register("PandasStrategy")
class PandasStrategy(DataStrategy):
    pass
#In this strategy, each configuration step will have to define where it wants to load and write data
#Maybe we'll have some naming convention- like taskname_field- 
#so the config just has to say something like input_cols:[a,b], output_cols:[d]. 
#Realistically, the atom itself will have to define what kind of input it expects from the datastrategy too. 
