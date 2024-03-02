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
        if isinstance(col, list):
            matching_dname = self.col_dname_map[col[0]]
        else:
            matching_dname = self.col_dname_map[col]
        return self.dataframes[matching_dname][col]
    
    #We use the "length" index when setting
    def __setitem__(self, col, new_value):
        if isinstance(new_value, list):
            input_length = len(new_value)
        else:   
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
            
    def set_or_new_df(self, column_name, new_series, document_name):
        if len(new_series.shape) != 1:
            raise RuntimeError("Cannot set new dataframe with multiple columns")
        
        try:
            self[column_name] = new_series
        except RuntimeError:
            new_series.rename(column_name, inplace=True)
            #Create the new dataframe, rename the first column as desired, and set the internal maps. 
            new_df = pd.DataFrame(new_series)

            self.dataframes[document_name] = new_df
        
            self.col_dname_map[column_name] = document_name
            
            input_length = new_series.shape[0]
            self.len_dname_map[input_length] = document_name

    def drop(self, drop_column, axis, inplace):
        matching_dname = self.col_dname_map[drop_column]

        pre_length = self.dataframes[matching_dname].shape[0]
        
        df = self.dataframes[matching_dname]
        
        #Statements dreamt up by the genuinely deranged: 
        df.drop(df[df[drop_column] == False].index, axis=0, inplace=inplace)

        post_length = self.dataframes[matching_dname].shape[0]
        print(pre_length)
        print(post_length)
        ###probably recalculate the indices when we do that- at least
        self.len_dname_map[post_length] = self.len_dname_map[pre_length]
        del(self.len_dname_map[pre_length])

    def __repr__(self):
        return f"<DataCollage with cols: {self.columns} between {len(self.dataframes)} DataFrames>"
            
