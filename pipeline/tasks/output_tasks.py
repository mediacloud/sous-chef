import pandas as pd
import os
from ..flowatom import FlowAtom
from .utils import lazy_import
matplotlib = lazy_import("matplotlib")

class OutputAtom(FlowAtom):
    """ 
    Output Atoms take a list of columns as a parameter, rather than the normal input/output situation
    Subclasses are left to impliment the actual logic about what to do with those columns
    
    """
    columns:list
        
    def pre_task(self):
        self.data = self.get_columns(self.columns)


#Output CSV with selected columns
#Render a default pandas graph of columns 
@FlowAtom.register("OutputCSV")
class OutputCSV(OutputAtom):
    
    output_location:str
    
    def task_body(self):
        
        location, filetype = self.output_location.split(".")
        i = 0
        while os.path.exists(f"{location}_{i}.{filetype}"):
            i += 1
        
        self.data.to_csv(f"{location}_{i}.{filetype}")


@FlowAtom.register("OutputFieldHistogram")
class OutputFieldHist(OutputAtom):
    
    output_location:str
    
    def task_body(self):
        figure = self.data.value_counts().plot.bar().get_figure()
        figure.savefig(self.output_location)
        
        
@FlowAtom.register("OutputSeriesHistogram")
class OutputSeriesHistogram(OutputAtom):
    
    output_location:str
    date_index_column:str
    values_column:str
    resample_str:str
    _defaults:{
        "resample_str":"T"
    }
    
    def task_body(self):
        df = self.data
        
        df["__date"] = df[self.date_index_column].astype("datetime64")
        df.set_index('__date', inplace=True)
        
        ax = matplotlib.pyplot.gca()
        values = set(df[self.values_column].values)
        for val in values:
            df.where(df[self.values_column]==val).resample(self.resample_str)[self.values_column].count().plot(kind="line", ax=ax)
        
        matplotlib.pyplot.legend(values)
        matplotlib.pyplot.savefig(self.output_location)