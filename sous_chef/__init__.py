from prefect import flow, task, get_run_logger
from .flowatom import FlowAtom
from prefect.runtime import flow_run
from .constants import DATASTRATEGY, NOSTRAT, DATA, ID, STEPS, PARAMS, INPUTS, OUTPUTS, NEWDOCUMENT, USER_CONFIGURED_OUTPUT, RETURNS
from .datastrategy import DataStrategy
from .exceptions import ConfigValidationError, NoDiscoveryError
from .tasks import *
from typing import List
import logging
from pprint import pprint 

def get_pipeline_runname():
    params = flow_run.parameters
    if "config" in params:
        if "NAME" in params["config"]:
            return f"{params['config']['NAME']}-PIPELINE"
    else:
        return "Sous-Chef-PIPELINE"

@flow(flow_run_name=get_pipeline_runname)
class Pipeline():
    """Core pipeline class. 
    Initialize it with a configuration json, and it will run all of the pre-run validation.
    Then call the class to run the pipeline
    """
    
    def __init__(self, config, run=True, log_level = "INFO"):
        
        #Configure logging preferences
        self.logger = get_run_logger()
        self.log_level = getattr(logging, log_level.upper())        
        self.logger.setLevel(self.log_level)

        
        #store the config file
        self.config = config
        
        #Do setup
        self.__get_atom_meta()
        self.__validate_and_setup_data()
        self.__validate_and_setup_steps()
        self.__validate_whole_flow()
        
        self.return_value = {}
        
        #Run the thing
        if run:
            self.logger.info("Setup complete, beginning sous chef execution")
            self.run_pipeline()
                
    def __get_atom_meta(self):
        #Some atom metadata (like- does it create or extend a document) is required
        #at the datasetup step- which, is obligated to run before the atom instantiation 
        #SO catch22- how can we get this information before we instantiate the atoms?
        available_atoms = FlowAtom.get_atoms()
        for flowatom in self.config[STEPS]:
            if flowatom[ID] in available_atoms:
                wrapped_class = available_atoms[flowatom[ID]].__wrapped__
                flowatom[NEWDOCUMENT] = wrapped_class.creates_new_document()
            else:
                raise ConfigValidationError(f"{flowatom[ID]} is not a registered Flow Atom")
        
        
    #In which we add the datastrategy specific config into the user supplied config
    def __validate_and_setup_data(self):
        available_strategies = DataStrategy.get_strats()
        
        if DATASTRATEGY in self.config:
            strat_name = self.config[DATASTRATEGY][ID]
            if strat_name in available_strategies:
                self.config = available_strategies[strat_name].setup_config(self.config)
                
            else:
                raise ConfigValidationError(f"{strat_name} is not a registered Data Strategy")
        else:
            #Use the backup nostrategy strategy
            self.config = available_strategies[NOSTRAT].update_config(self.config)
    
    #Do a top-level validation of the configuration parameters first, then initialize all the steps 
    def __validate_and_setup_steps(self):
        available_atoms = FlowAtom.get_atoms()
        for flowatom in self.config[STEPS]:
            if flowatom[ID] not in available_atoms:
                raise ConfigValidationError(f"{flowatom[ID]} is not a registered Flow Atom")
        
        #Then create initialize the steps, triggering lower-level validation
        all_return_names = []
        self.steps = []
        for flowatom in self.config[STEPS]:
            if RETURNS not in flowatom:
                returns = {}
                
            else:
                returns = flowatom[RETURNS]
                all_return_names.extend(returns.items())
                

            #Instantiate flow atom
            step = available_atoms[flowatom[ID]](flowatom[PARAMS], flowatom[DATA], returns, log_level=self.log_level)
            self.steps.append(step)
            
        if len(set(all_return_names)) != len(all_return_names):
            raise ConfigValidationERror(f"There's a collision in the return value names")
        
        
    
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
                        raise ConfigValidationError(f"input {ds_name} does not correspond to any defined outputs")
                    
                    input_type = step.task_inputs[function_name]
                    output_type = output_type_map[ds_name]
                    
                    if input_type != output_type and input_type is not None:
                        raise ConfigValidationError(f"{name} input {ds_name} expects type {input_type}, but is {output_type}")
                    
        
    
    def run_pipeline(self):
        
        for step in self.steps:
            try:
                return_value = step()
            except NoDiscoveryError:
                self.logger.warn("Discovery Atom found no content, no work to do!")
                break
            else:
                if return_value:
                    for key, item in return_value.items():
                        self.return_value[key] = item

            
#This is the main entrypoint for the whole thing            
def RunPipeline(config, **kwargs):
    pipeline = Pipeline(config, **kwargs)
    
    #return pipeline.return_value
    

@flow()
def get_documentation():
    all_docs = {}
    for name, atom in FlowAtom.get_atoms().items():
        
        docs = atom(None, None, document=True).docs
        all_docs[name] = docs
    return all_docs
 