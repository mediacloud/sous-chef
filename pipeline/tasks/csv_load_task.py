from ..flowatom import FlowAtom
import pandas as pd


@FlowAtom.register("ReadCSV")
class read_csv(FlowAtom):
    
    location:str
    columns:dict
    
    @classmethod
    def creates_new_document(self):
        return True
    
    def setup_hook(self, params, data_config):
        self.task_outputs = params["columns"]
    
    def outputs(self, configured__:None):pass
    
    def task_body(self):
        df = pd.read_csv(self.location)
        self.results = df