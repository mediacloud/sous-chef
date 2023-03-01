import pandas as pd

#A wrapper over multiple pandas dataframes that lets us transparently access individual documents. 

class DataCollage(object):
    def __init__(self, dfs):
        self.force_new_columns = False
        self.dataframes = dfs
        
        #We should be able to list out all of our columns
        self.columns = []
        #And we need access via two kinds of indexes- one by column name
        self.col_dname_map = {}
        #and one by document lengths
        self.len_dname_map = {}
        
        #Just set those up and watch out for name overlap
        for docname, dataframe in self.dataframes.items():
            for col in dataframe.columns:
                if col in self.columns:
                    raise RuntimeError(f"Column {col} appears in two input documents")
                self.columns.append(col)
                self.col_dname_map[col] = docname
                self.len_dname_map[dataframe.shape[0]] = docname
        
        #And then use the setattr magic so that this class works like an individual df on the surface
        for col, dname in self.col_dname_map.items():
            setattr(self, col, self.dataframes[dname][col])
            
    def __getitem__(self, col):
        #Just go through the column index
        matching_dname = self.col_dname_map[col]
        return self.dataframes[matching_dname][col]
    
    def __setitem__(self, col, new_value):
        #Go through the length index
        input_length = new_value.shape[0]
        if input_length in self.len_dname_map:
            matching_dname = self.len_dname_map[input_length]
            self.dataframes[matching_dname][col] = new_value
            if col not in self.columns:
                #make sure to update all the indices when we setitem
                
                self.col_dname_map[col] = matching_dname
                self.len_dname_map[input_length] = matching_dname
                
                self.columns.append(col)
                setattr(self, col, self.dataframes[matching_dname][col] )
                
        else:
            raise RuntimeError(f"Cannot set {col} with this value, as No columns of length {new_value.shape[0]} exist. Try dc.set_or_new_df")
            
    def set_or_new_df(self, col, new_value, dname):
        if len(new_value.shape) != 1:
            raise RuntimeError("Cannot set new dataframe with multiple columns")
        
        try:
            self[col] = new_value
        except RuntimeError:
            #Create the new dataframe, rename the first column as desired, and set the internal maps. 
            new_df = pd.DataFrame(new_value)
            
            old_name = new_df.columns[0]
            new_df.rename({old_name:col})
            
            self.dataframes[dname] = new_df
        
            self.col_dname_map[col] = dname
            
            input_length = new_value.shape[0]
            self.len_dname_map[input_length] = dname
            