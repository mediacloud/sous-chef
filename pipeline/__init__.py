from prefect import flow
from .flowatom import FlowAtom
from .datastrategy import DataStrategy, DATASTRATEGY, NOSTRAT, DATA
from .tasks import *


ID = "id"
STEPS = "steps"
PARAMS = "params"

#This guy manages the overall pacing of the pipeline.
#Environment setup, variables, data strategies, etc. 
@flow()
class Pipeline():
    def __init__(self, config):
        self.config = config
        self.__validate_and_setup_data()
        self.__validate_and_setup_steps()

    
    #In which we add the datastrategy specific config into the user supplied config
    def __validate_and_setup_data(self):
        if DATASTRATEGY in self.config:
            available_strategies = DataStrategy.get_strats()
            strat_name = self.config[DATASTRATEGY][ID]
            if strat_name in available_strategies:
                self.config = available_strategies[strat_name].update_config(self.config)
            else:
                raise RuntimeError(f"{strat_name} is not a registered Data Strategy")
        else:
            self.config = available_strategies[NOSTRAT].update_config(self.config)
    
    #Do a top-level validation first
    def __validate_and_setup_steps(self):
        available_atoms = FlowAtom.get_atoms()
        for task in self.config[STEPS]:
            if task[ID] not in available_atoms:
                raise RuntimeError(f"{task[ID]} is not a registered Flow Atom")
         
        #Then create all the lower-level validation things
        self.steps = [available_atoms[task[ID]](task[PARAMS], task[DATA]) for task in self.config[STEPS]]
        
    
    def __call__(self):
        for step in self.steps:
            step()

            
#This is the main entrypoint for the whole thing            
@flow()
def RunPipeline(config):
    test_pipeline = Pipeline(config)
    test_pipeline()