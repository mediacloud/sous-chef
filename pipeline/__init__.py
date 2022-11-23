from prefect import flow

from .flowatom import FlowAtom
from . import Tasks 


ID = "id"

#This guy manages the overall pacing of the pipeline.
#Environment setup, variables, data strategies, etc. 
@flow()
class Pipeline():
    def __init__(self, config):
        
        self.config = config
        self.__validate()
        available_atoms = FlowAtom.get_atoms()
        #We create all the atoms up top so they run their validation against the provided configuration
        self.steps = [available_atoms[task[ID]](task["params"]) for task in self.config]
    
    def __validate(self):
        available_atoms = FlowAtom.get_atoms()
        for task in self.config:
            if task[ID] not in available_atoms:
                raise RuntimeError(f"{task[ID]} is not registered")
    
    def __call__(self):
        for step in self.steps:
            step()

            
#This is the main entrypoint for the whole thing            
@flow()
def RunPipeline(config):
    test_pipeline = Pipeline(config)
    test_pipeline()