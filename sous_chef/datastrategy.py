import pandas as pd
from pandas.errors import EmptyDataError
import numpy as np
import inspect
import os
import ast
import json
import hashlib
import shutil
import copy
from typing import List, Dict
from prefect import get_run_logger
from pprint import pprint
from .datacollage import DataCollage
from .exceptions import ConfigValidationError
from .constants import (ID, STEPS, DATA, DATASTRATEGY, DATALOCATION, READLOCATION, 
    WRITELOCATION, INPUTS, OUTPUTS, PARAMS, RUNNAME, NOSTRAT, NEWDOCUMENT, DOCUMENTMAP, 
    USER_CONFIGURED_OUTPUT, USER_CONFIGURED_COLUMNS, STRING_TYPE_MAP, LOAD_IF_CACHED,
    CACHE_STEP, CACHE_SKIP, CACHE_LOAD, CACHE_HASHES, CACHE_SAVE)


#There should be one data strategy per pipeline
#Each impliments three methods:

class DataStrategy(object):
    _REGISTERED_STRATS = {}
    
    @classmethod
    def get_strats(cls):
        return cls._REGISTERED_STRATS
    
    #This lets us register new data strats just like with the pipeline atoms
    @classmethod
    def register(cls, _name):
        
        def _register(stratclass):
            cls._REGISTERED_STRATS[_name] = stratclass
            return stratclass 
        return _register
    
    def __init__(self, config, function_inputs, function_outputs):
        self.logger = get_run_logger()
        
        if config is not None:
            if INPUTS in config and config[INPUTS] is not None:
                config_inputs = config[INPUTS].keys()
                
                for i in config_inputs:
                    if i not in function_inputs:
                        raise ConfigValidationError(f"Atom has no input named {i}")
                
                for i in function_inputs:
                    if i not in config_inputs:
                        raise ConfigValidationError(f"Atom expects input named {i}")
                
                
                
            if OUTPUTS in config and config[OUTPUTS] is not None:
                config_outputs = config[OUTPUTS].keys()
                if USER_CONFIGURED_OUTPUT not in function_outputs:
                    for i in config_outputs:    
                        if i not in function_outputs:
                            raise ConfigValidationError(f"Atom has no output named {i}")
            
            
            for key, value in config.items():
                setattr(self, key, value)

        self.config = config
        self.function_inputs = function_inputs
        self.function_outputs = function_outputs
    
    # a class method update_config which reads a pipeline configuration 
    #and backfills each step with the information the strategy will need at each step
    @classmethod
    def setup_config(cls, config):
        pass
    
    #A generator which yeilds data 
    def get_data(self):
        pass  
    
    #A method which writes data 
    def write_data(self, data):
        pass
    
@DataStrategy.register(NOSTRAT)
class NoStrategy(DataStrategy):
    """Dummy empty strategy class, to be subsituted when no other strat is being used. """
    
    @classmethod
    def update_config(self, config):
        for i, step in enumerate(config[STEPS]):
            step[DATA] = None
            
        return config
    
    def get_data(self):
        return None
    
    def write_data(self, data):
        return None
    

#In this strategy, each configuration step will have to define where it wants to load and write data
#Maybe we'll have some naming convention- like taskname_field- 
#so the config just has to say something like input_cols:[a,b], output_cols:[d]. 
@DataStrategy.register("PandasStrategy")
class PandasStrategy(DataStrategy):
    """A strategy which uses CSVs on the filesystem, indexed with pandas, to store data in-between flow steps"""
    
    @classmethod
    def setup_config(self, config):
        logger = get_run_logger()
        #For when outputs are configured by runtime- look for a parameter named COLUMN and try to infer things from there
        for step in config[STEPS]:
            if OUTPUTS in step:
                if USER_CONFIGURED_OUTPUT in step[OUTPUTS]:
                    step[OUTPUTS] = copy.copy(step[PARAMS][USER_CONFIGURED_COLUMNS])
                    for name, type_string in step[OUTPUTS].items():
                        step[OUTPUTS][name] = name
                   
                        
        #Iterate through the configuration to figure out which document each field belongs to
        #If a field is output by a new_document atom, just use that 
        #Otherwise, do a one-step backtrace to see where the previous input lives and use that. 
        #Basically, we always assume that the output matches the input, unless otherwise stated.
        #In the case where an atom has two unlike inputs? Well fuck around and find out, I suppose:
        #Probably just refer to that as a new document as well
        field_to_document_map = {}
        for step in config[STEPS]:
            if OUTPUTS in step:
                if(step[NEWDOCUMENT]):
                    for output in step[OUTPUTS].values():
                        field_to_document_map[output] = step[ID]
                else:
                    if INPUTS in step:
                        to_find = list(step[INPUTS].values())
                        doc_match = field_to_document_map[to_find[0]]
                        for output in step[OUTPUTS].values():
                            field_to_document_map[output] = doc_match
        #And use this mapping later on to make sure values are loaded/saved to the right place

        #format is data_root/runname/documentname_output.csv
        data_location_root = config[DATASTRATEGY][DATALOCATION]
        runname = config[RUNNAME]
        self.runname = runname
        
        if not os.path.exists(data_location_root):
            os.mkdir(data_location_root)
        
        #iterate so we don't overwrite old runs unintentionally
        run_number = 0
        while os.path.exists(f"{data_location_root}{runname}-{run_number}"):
            run_number += 1
        data_directory = f"{data_location_root}{runname}-{run_number}/"
        os.mkdir(data_directory)
        self.run_number = run_number
        
        
        #create the documents
        documents = list(set(field_to_document_map.values()))
        for doc in documents:
            doc_loc = data_directory+doc+"_output.csv"
            start_dataframe = pd.DataFrame()
            start_dataframe.to_csv(doc_loc)
            
        self.cache_index = None 
        caching_params = []
        for i, step in enumerate(config[STEPS]):
            
            data_meta = {
                DATASTRATEGY: config[DATASTRATEGY][ID],
                DATALOCATION: data_directory,
                INPUTS: step[INPUTS] if INPUTS in step else None,
                OUTPUTS: step[OUTPUTS] if OUTPUTS in step else None,
                DOCUMENTMAP: field_to_document_map,
                CACHE_STEP: None
                
            }
            
            #Build the caching hash
            if self.cache_index is None:
                caching_params.extend(
                    [str(step[PARAMS]), str(data_meta[INPUTS]), str(data_meta[OUTPUTS])]
                )
                  
            
            if LOAD_IF_CACHED in step:
                if self.cache_index is None:
                    self.cache_index = i
                else:
                    raise ConfigValidationError("Only one cache point can be specified per configuration")
            
            step[DATA] = data_meta
        
        #Reset the caching params, if there are none to be found
        if self.cache_index == None:
            caching_params = []
        
        self.cache_hash = hash_list_or_none(caching_params)
        
        #Load or instantiate the config metadata file
        self.config_meta = f"{data_location_root}{runname}-meta.json"
        if os.path.exists(self.config_meta):
            config_metadata = json.load(open(self.config_meta, "r"))
        else:
            config_metadata = new_config_meta()
            json.dump(config_metadata, open(self.config_meta, "w"))
        
        #if the params are empty, the config never calls for caching so just ignore it
        if caching_params == []:                      
            do_cache = False
        #if the hash is not none but it isn't in the metadata, then there's no cache to load from
        elif self.cache_hash not in config_metadata[CACHE_HASHES]:
            do_cache = False
        #otherwise, we cache. 
        else:
            do_cache = True
        
       
        if do_cache:
            logger.warn("This run will use a cache")
            
            cached_documents = []
            #Set Cache behavior meta on each step
            for i, step in enumerate(config[STEPS]):
                #cached_documents.extend(step[DATA][DOCUMENTMAP].values())
                if i < self.cache_index:
                    cached_documents.extend(step[DATA][DOCUMENTMAP].values())
                    step[DATA][CACHE_STEP] = CACHE_SKIP
                elif i == self.cache_index:
                    cached_documents.extend(step[DATA][DOCUMENTMAP].values())
                    step[DATA][CACHE_STEP] = CACHE_LOAD 
            #Also copy the cached documents into the new data_directory
            cache_location = config_metadata[CACHE_HASHES][self.cache_hash]
            cache_directory = "/".join(cache_location.split("/")[:-1])
            cached_documents = list(set(cached_documents))
            for location in cached_documents:
                original = cache_directory + "/" + location + "_output.csv"
                new = data_directory + location + "_output.csv"
                shutil.copy(original, new)
            
        #Then, if we're not loading from cache but a cache_index is set, then we need that step to save its output to a cache
        elif self.cache_index is not None:
            config[STEPS][self.cache_index][DATA][CACHE_STEP] = CACHE_SAVE
            
        return config
    
                               
    #Just read from the metadata file
    def find_cached_output(self):
        config_metadata = json.load(open(self.config_meta, "r"))
        return config_metadata[CACHE_HASHES][self.cache_hash]
        
        
    def get_data(self, cache=False):
        document_map = self.config[DOCUMENTMAP]
        read_locations = [inputmap[1] for inputmap in self.inputs.items()]
        documents = list(set([document_map[_in] for _in in read_locations]))
        if len(documents) == 1:
            if cache:
                self.logger.info("Loading input from cache")
                doc_loc = self.find_cached_output()
            else:
                doc_loc = self.data_location+documents[0]+"_output.csv"
            
            read_dataframe = read_or_empty_dataframe(doc_loc)
            operating_dataframe = pd.DataFrame()

            for function_name, read_location in self.inputs.items():
                #apply literal eval so that types are preserved, if we're not just loading a string
                expected_dtype = self.function_inputs[function_name]

                value =  read_dataframe[read_location].apply(lambda x:eval_or_nan(x, expected_dtype))

                operating_dataframe[function_name] = value
            
            results_dataframe = pd.DataFrame()
            if self.outputs is not None:
                #This just sets up the outputs so that the flow object can write to it with dot syntax
                for function_output_name in self.function_outputs.keys():
                    results_dataframe[function_output_name] = pd.Series(dtype=object)
   
        else:
            raise RuntimeError("Multidocument reads are not implimented yet")
        return operating_dataframe, results_dataframe

    
    
    def write_data(self, operating_dataframe, cache=False):
        self.logger.info("Writing dataframe")
        if self.outputs is not None:
            document_map = self.config[DOCUMENTMAP]

            write_locations = [outputmap[1] for outputmap in self.outputs.items()]
            documents = list(set([document_map[out] for out in write_locations]))
            if len(documents) == 1:
                doc_loc = self.data_location+documents[0]+"_output.csv"

                #Load the df to begin with. 
                write_dataframe = read_or_empty_dataframe(doc_loc)
                for function_name, write_location in self.outputs.items():
                    
                    write_dataframe.set_or_new_df(write_location, operating_dataframe[function_name], doc_loc)
                    #write_dataframe[write_location] = operating_dataframe[function_name]

                #Use the datacollage access point to get each df
                for location, dataframe in write_dataframe.dataframes.items():
                    dataframe.to_csv(location)
                               
                if cache: #Confirm that we've written a reloadable cache now. 
                    self.logger.info("Writing output to cache")
                    config_metadata = json.load(open(self.config_meta, "r"))
                    config_metadata[CACHE_HASHES][self.cache_hash] = doc_loc
         
                    json.dump(config_metadata, open(self.config_meta, "w"))
            else:
                raise RuntimeError("Multidocument writes are not supported")
        else:
            raise RuntimeError("Can't call write_data on an atom with no outputs defined")
                         
                         
    def apply_filter(self, keep_column):
        #keep_column should be a column of bools, where "true" means the row will be kept, and 'false' that it will be dropped
        
        documents = list(set([document_map[out] for out in write_locations]))
        if len(documents) == 1:
            doc_loc = self.data_location + documents[0] + "_output.csv"
            
            dataframe = read_or_empty_dataframe(doc_loc)

            dataframe["_KEEP"] = keep_column
            post_filter_dataframe = dataframe[dataframe["_KEEP"]]

            post_filter_dataframe.drop('_KEEP', axis=1, inplace=True)

            post_filter_dataframe.to_csv(self.data_location)
        else:
            raise RuntimeError("apply_filter to multiple documents is not yet supported")
        
    def get_columns(self, columns):
        document_map = self.config[DOCUMENTMAP]
        documents = list(set([document_map[_in] for _in in columns]))
        if len(documents) == 1:
            doc_loc = self.data_location+documents[0]+"_output.csv"
            dataframe = read_or_empty_dataframe(doc_loc)
            subset = dataframe[columns]
        else:
            raise RuntimeError("returning columns from multiple documents is not yet supported")
        return subset
        

#The python engine, which is needed to read/write multilingual strings I think,
#Doesn't like empty files. so just wrap the call to catch the exception
def read_or_empty_dataframe(data_location):
    try:
        df = pd.read_csv(data_location, engine="python")
        dc = DataCollage({data_location:df})
        return dc
    except EmptyDataError:
        df =  pd.DataFrame()
        dc = DataCollage({data_location:df})
        return dc

        
#Apply this to csvs when read from disk to restore pythonic types contained within
def eval_or_nan(val, expected_dtype):
    if expected_dtype == None:
        return val
    if expected_dtype == str:
        return val
    
    if str(val) == "nan":
        return val
    if not isinstance(val, str):
        #print("isinstance")
        #print(ast.literal_eval(val))
        return ast.literal_eval(str(val))
    else:
        return ast.literal_eval(val)


def hash_list_or_none(to_hash):
    if to_hash is not None:
        hashable = ",".join(i for i in to_hash)
        hash_object = hashlib.sha1(hashable.encode())
        return hash_object.hexdigest()
    else:
        return None
    

#Schema for the config metadata file. If this ends up ever doing any more work than just managing cache data
# I should consider just implimenting it as a class so I don't have to read/write so often. 
def new_config_meta():
    return {CACHE_HASHES:{}}
