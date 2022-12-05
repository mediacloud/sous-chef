from prefect import flow
from .flowatom import FlowAtom
from .constants import DATASTRATEGY, NOSTRAT, DATA, ID, STEPS, PARAMS, INPUTS, OUTPUTS
from .datastrategy import DataStrategy
from .tasks import *
from typing import List


#This guy manages the overall pacing of the pipeline.
#Environment setup, variables, data strategies, etc. 
@flow()
class Pipeline():
    
    self.steps:List[FlowAtom]
    
    def __init__(self, config):
        self.config = config
        self.__validate_and_setup_data()
        self.__validate_and_setup_steps()
        self.__validate_whole_flow()

    #In which we add the datastrategy specific config into the user supplied config
    def __validate_and_setup_data(self):
        available_strategies = DataStrategy.get_strats()
        if DATASTRATEGY in self.config:
            strat_name = self.config[DATASTRATEGY][ID]
            if strat_name in available_strategies:
                self.config = available_strategies[strat_name].setup_config(self.config)
            else:
                raise RuntimeError(f"{strat_name} is not a registered Data Strategy")
        else:
            self.config = available_strategies[NOSTRAT].update_config(self.config)
    
    #Do a top-level validation of the configuration parameters first, then initialize all the steps 
    def __validate_and_setup_steps(self):
        available_atoms = FlowAtom.get_atoms()
        for task in self.config[STEPS]:
            if task[ID] not in available_atoms:
                raise RuntimeError(f"{task[ID]} is not a registered Flow Atom")
         
        #Then create all the lower-level validation things
        self.steps = [available_atoms[task[ID]](task[PARAMS], task[DATA]) for task in self.config[STEPS]]
        
    
    #Do a validation of the way the atoms are plugged into one another here. 
    def __validate_whole_flow(self):
        #Iterate through the config, and get the expected type for each column name at each step. 
        output_type_map = {}
        
        #Note all the output types
        for i, step in enumerate(self.steps):
            if OUTPUTS in self.config[STEPS][i]:
                outputs = self.config[STEPS][i][OUTPUTS]
                for function_name, ds_name in outputs.items():
                    output_type_map[ds_name] = step.task_outputs[function_name]
        
        #Then iterate through the steps again and make sure that the inputs all equal the expected outputs
        for i, step in enumerate(self.steps):
            if INPUTS in self.config[STEPS][i]:
                name = self.config[STEPS][i][ID]
                inputs = self.config[STEPS][i][INPUTS]
                for function_name, ds_name in inputs.items():
                    if ds_name not in output_type_map:
                        raise RuntimeError(f"Configuration Error: input {ds_name} does not correspond to any defined outputs")
                    
                    input_type = step.task_inputs[function_name]
                    output_type = output_type_map[ds_name]
                    
                    if input_type != output_type and input_type is not None:
                        raise RuntimeError(f"Configuration Error: {name} input {ds_name} expects type {input_type}, but is {output_type}")
                    
        
    
    def __call__(self):
        for step in self.steps:
            step()

            
#This is the main entrypoint for the whole thing            
@flow()
def RunPipeline(config):
    test_pipeline = Pipeline(config)
    test_pipeline()