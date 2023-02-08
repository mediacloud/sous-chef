import pandas as pd
import os
from ..flowatom import FlowAtom
from .utils import lazy_import
import matplotlib.pyplot as plt

class OutputAtom(FlowAtom):
    """ 
    Output Atoms take a list of columns as a parameter, rather than the normal input/output situation
    Subclasses are left to impliment the actual logic about what to do with those columns
    
    """
    columns:list
        
    def pre_task(self):
        self.data = self.get_columns(self.columns)



@FlowAtom.register("OutputCSV")
class OutputCSV(OutputAtom):
    """
    Outputs a CSV which includes the given columns
    """
    output_location:str
    
    def task_body(self):
        
        filetype = self.output_location.split(".")[-1]
        i = 0
        while os.path.exists(f"{self.output_location[:-4]}_{i}.{filetype}"):
            i += 1
        
        self.data.to_csv(f"{self.output_location[:-4]}_{i}.{filetype}")


@FlowAtom.register("OutputFieldHistogram")
class OutputFieldHist(OutputAtom):
    """
    Outputs a histogram of values in a single column
    """
    output_location:str
    
    def task_body(self):
        figure = self.data.value_counts().plot.bar().get_figure()
        figure.savefig(self.output_location)
        
        
@FlowAtom.register("OutputTimeSeriesHistogram")
class OutputSeriesHistogram(OutputAtom):
    """
    Outputs a histogram of the values of a column over time- useful for displaying sentiment over time. 
    """
    
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
        
        
        _max = 0
        ax = plt.gca()
        values = set(df[self.values_column].values)
        for val in values:
            df.where(df[self.values_column]==val).resample(self.resample_str)[self.values_column].count().plot(kind="line", ax=ax)
            m = df.where(df[self.values_column]==val).resample("T")[self.values_column].count().max()
            if m > _max:
                _max = m 
        
        ax.set_ylim([-10, _max+10])
        plt.legend(values)
        plt.savefig(self.output_location)