from ..flowatom import FlowAtom
import pandas as pd
import itertools

encodings = ["utf-8", "utf-16" ]
seps = [",", "\t"]

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

        #So, we know that, for example, tableau "csv" files are actually utf-16 encoded tab-separated files. 
        #so we should have some kind of backup system 
        df = try_load_permutations(self.location)
        self.results = df
        

#Just a fallback loop to try and load files with various combinations of encodings
def try_load_permutations(location):
    params = itertools.product(encodings, seps)
    loaded = False
    
    for encoding, sep in params:
        try:
            df = pd.read_csv(location, encoding=encoding, sep=sep)
            loaded = True
        except Exception as e:
            continue
    
    if loaded:
        return df
    else:
        raise RuntimeError(f"Could not load {location}- encoding not supported")