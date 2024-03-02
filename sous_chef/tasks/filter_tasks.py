from ..flowatom import FlowAtom
import pandas as pd

class ReduceFilterAtom(FlowAtom):
    """
    Atoms which subclass this can specify a 'filter_keep_mask' instead of standard outputs. 
    Documents which are tagged as 'false' in this mask will be fully dropped
    from the underlying datastore. THIS THROWS OUT DATA WHICH FAILS A FILTER. 
    Some other solution will be needed if we ever want to, say, apply a pipeline step only to a filtered
    subset of the data that we want to keep around
    """
    
    #def pre_task(self):
        #self.data = self.get_data()
        #print(self.data)
        #print(dir(self.data))
        #self.filter_keep_mask = [True] * len(self.data.index)
    
    def post_task(self):
        #self.filter_keep_mask = pd.Series(self.filter_keep_mask)
        self.apply_filter(self.filter_keep_mask)


@FlowAtom.register("FilterBelowN")
class FilterBelowNTask(ReduceFilterAtom):
    """
    Filter out rows where the input 'to_compare' is below the parameter n
    """
    n:int
    
    def inputs(self, to_compare:int):pass
    
    def task_body(self):
        
        self.filter_keep_mask = [dp > self.n for dp in self.data.to_compare]
        

@FlowAtom.register("FilterMCStoryIDs")
class FilterMCStoryIDs(ReduceFilterAtom):

    ids_to_remove: list

    def inputs(self, ids:str):pass

    def task_body(self):
        
        self.filter_keep_mask = [id not in self.ids_to_remove for id in self.data.ids]