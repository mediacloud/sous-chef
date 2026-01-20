from ..flowatom import FlowAtom
from ..constants import STRING_TYPE_MAP
import pandas as pd
import itertools
from pprint import pprint

encodings = ["utf-8", "utf-16" ]
seps = [",", "\t"]

@FlowAtom.register("ReadCSV")
class read_csv(FlowAtom):
    """ Read in the contents of a CSV file. Set "location" to the relative location of your input file, 
    and "columns" to the names and types of the columns you want to load into the pipeline.
    REQUIRES the unique "outputs: configured__" tag, since we need to overwrite the standard config behavior
    
    """
    location:str
    columns:dict
    
    @classmethod
    def creates_new_document(self):
        return True
    
    #Sets the atom outputs to the values set in the configuration parameters
    def setup_hook(self, params, data_config):
        
        typed_params = {k: STRING_TYPE_MAP[v] for k, v in params['columns'].items()}

        self.task_outputs = typed_params
    
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
        raise RuntimeError(f"Could not load {location}- encoding not supported. Contact mantainer for support")