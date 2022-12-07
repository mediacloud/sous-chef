from ..flowatom import FlowAtom


class ReduceFilterAtom(FlowAtom):
    #Atoms which subclass this can specify a 'filter_keep_mask'
    #Documents which are tagged as 'false' in this mask will be fully dropped
    #from the underlying datastore. THIS THROWS OUT data which fails a filter.
    #Some other solution will be needed if we ever want to, say, apply a pipeline step only to a filtered
    #subset of the data that we want to keep around
    
    def pre_task(self):
        self.data = self.get_data()
        self.filter_keep_mask = [True] * len(self.data.index)
    
    def post_task(self):
        self.apply_filter(self.filter_keep_mask)



@FlowAtom.register("FilterBelowN")
class FilterBelowNTask(ReduceFilterAtom):
    
    n:int
    
    def inputs(self, to_compare:int):pass
    
    def task_body(self):
        
        self.filter_keep_mask = [dp > self.n for dp in self.data.to_compare]
        

    
    