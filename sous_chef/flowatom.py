import inspect
from pydantic import BaseModel
from prefect import flow, task, get_run_logger
from uuid import uuid4
import logging
from .exceptions import ConfigValidationError
from .datastrategy import DataStrategy
from .constants import DATA, DATASTRATEGY, NOSTRAT, DEFAULTS, CACHE_STEP, CACHE_SKIP, CACHE_LOAD, CACHE_SAVE, STRING_TYPE_MAP, OUTPUTS, RETURNS
"""
This is the magic class which performs most of the mucking about with python innards
in order to specify a nice encapsulated and validatable confuguration vocabulary
"""


class FlowAtom(object):
    #Silly singleton pattern lets us register subclasses to this parameter
    _REGISTERED_ATOMS = {}

    task_name:str
    restricted: bool
    _defaults:{
        "task_name":"default"
        "restricted": False
    }
    _new_document:False
    
    def __init__(self, params, data_config, returns, document=False, log_level="INFO"):
        self.return_values = {} #Can be set by a task.
        self.log_level = log_level
        if not document:
            self.task_inputs = inspect.get_annotations(self.inputs)
            self.task_outputs = inspect.get_annotations(self.outputs)
            
            #Install the datastrategy, and validate our params, and setup return values
            self.__setup_strategy(data_config)
            self.__validate_and_apply(params)
            self.__validate_returns(returns)
            
        
            #A place to define unique setup per flow atom
            self.setup_hook(params, data_config)
            
            #Give the task a unique name, if not set by the recipe
            if self.task_name == "default":
                self.task_name = self.__class__.__qualname__ + "-"+uuid4().hex[:2]
            
            
        else:
            self.warn("A pipeline constructed with document==True cannot be used for any data analysis task- this is only for generating documentation")
            self.docs = self.document()
            

    def info(self, msg):
        self.logger.info(msg)

    def warn(self, msg):
        self.logger.warning(msg)
    
    #Easy Access to the subclass registry 
    @classmethod
    def get_atoms(cls):
        return cls._REGISTERED_ATOMS
    
    #This takes care of registering the atom locally for our pipeline project,
    #and also running the prefect registration hook. 
    @classmethod
    def register(cls, name):
        
        def _register(stepclass):
            cls._REGISTERED_ATOMS[name] = stepclass#, name=f"{name}-setup")
            return stepclass 
        
        return _register
    
    @classmethod
    def creates_new_document(self):
        return False
    
    
    #MRO = "Method Resolution Order" - basically the class inheritance tree. 
    #This walks the MRO and grabs all the annotations that are defined on it
    #Validates the provided parameters, and sets the values to the object
    def __validate_and_apply(self, params):
        
        #Gather all annotations and defaults from the MRO
        all_annotations = {}
        all_defaults = {}
        for cls in type(self).mro():
            for name, value in inspect.get_annotations(cls).items():
                if name == DEFAULTS:
                    all_defaults.update(value)
                elif name[0] != '_':
                    all_annotations.update({name:value})
        
        
        #Validate and set parameters which are provided by the configuration
        set_params = []
        for key, value in params.items():
            if key not in all_annotations.keys():
                raise ConfigValidationError(f"{key} is not a valid configuration key. Options are: {all_annotations.keys()}")
            if not isinstance(value, all_annotations[key]):
                raise ConfigValidationError(f"Bad Configuration, {key} must be {all_annotations[key]}")
            
            set_params.append(key)    
            setattr(self, key, value)
        
        #Try to apply defaults for parameters which are unconfigured
        for key, value in all_annotations.items():
            if key not in set_params:
                if key not in all_defaults:
                    raise ConfigValidationError(f"Bad Configuration, missing required parameter {key}:{value}")
                else:
                    setattr(self, key, all_defaults[key])

        for cls in type(self).mro():
            if('validate') in dir(cls):
                cls.validate(self)
        
        self.validate()

    def __setup_strategy(self, data_config):
        available_strategies = DataStrategy.get_strats()
        
        if data_config is None:
            strat_name = NOSTRAT
            
        else:   
            strat_name = data_config[DATASTRATEGY]
        self.__data_strategy_name = strat_name
        if strat_name in available_strategies:
            self.__data_strategy = available_strategies[strat_name](data_config, self.task_inputs, self.task_outputs)
        else:
            raise ConfigValidationError(f"Bad Configuration: {strat_name} is not a valid data strategy")
        self.data_config = data_config
        self.cache_behavior = data_config[CACHE_STEP]
    
    def __validate_returns(self, returns):
        for val in returns:
            if val not in self.data_config[OUTPUTS]:
                raise ConfigValidationError(f"Bad Configuration, cannot return from uninitialized output {val}")

        self.return_keys = returns
    
    
    
    def setup_hook(self, params, data_config):
        #A hook for any task-specific overrides to the setup steps 
        pass
        
    #The details of the specific task are implimented here
    def task_body(self):
        raise RunTimeError("task_body is Unimplimented")
    
    def validate(self): pass 
        #A method than can be called by a flow atom to perform more fine-grained validation than simple type-checking can do
    
    def inputs(self): pass
        #This might be un-pythonic, but we are using this function to define the input schema for the atom
        #All values in the input schema for this function must be defined for a configuration to pass validation
        
    def outputs(self): pass
        #Same as above- this function defines the output schema for the atom. 
        #No values that are not defined in this schema can be defined for a configuration to pass validation-
        #ie- these are the many optional outputs
    
    
    def get_data(self, cache=False):
        if self.__data_strategy == None:
            raise RuntimeError("Cannot Use get_data if No Datastrategy Is Specified")
        else:
            return self.__data_strategy.get_data(cache=cache)
    
    def write_data(self, data, cache=False):
        if self.__data_strategy == None:
            raise RuntimeError("Cannot Use write_data if No Datastrategy Is Specified")
        else:
            return self.__data_strategy.write_data(data, cache=cache)
    
    
    def apply_filter(self, keep_filter):
        if self.__data_strategy == None:
            raise RuntimeError("Cannot Use apply_filter if No Datastrategy Is Specified")
        else:
            return self.__data_strategy.apply_filter(keep_filter)
    
    
    def get_columns(self, columns):
        if self.__data_strategy == None:
            raise RuntimeError("Cannot Use get_columns if No Datastrategy Is Specified")
        else:
            return self.__data_strategy.get_columns(columns)
    
    #This loads specified data to self.data as a dataframe
    def pre_task(self):
        self.logger = get_run_logger()
        self.logger.setLevel(self.log_level)
        if self.__data_strategy.inputs is not None:
            if self.cache_behavior == CACHE_LOAD:
                self.data, self.results = self.get_data(cache=True)
            else:
                self.data, self.results = self.get_data()
        
    #Task finish- write self.data to the dataframe
    def post_task(self):
        if self.__data_strategy.outputs is not None:
            #grab the value to return

            for task_key, return_name in self.return_keys.items():
                self.return_values[return_name] = self.results[task_key]
                
            if self.cache_behavior == CACHE_SAVE:
                self.write_data(self.results, cache=True)
            else:
                self.write_data(self.results)
            
        
    def document(self):
        #Gather all annotations and defaults from the MRO
        TYPE_STRING_MAP = {y:x for x,y in STRING_TYPE_MAP.items()}
        
        all_annotations = {}
        all_defaults = {}
        for cls in type(self).mro():
            for name, value in inspect.get_annotations(cls).items():
                if name == DEFAULTS:
                    all_defaults.update(value)
                elif name[0] != '_':
                    all_annotations.update({name:TYPE_STRING_MAP[value]}) 
        docstring = self.__doc__
        outputs_ = inspect.get_annotations(self.outputs)
        outputs = {}
        for name, type_ in outputs_.items():
            if type_ in TYPE_STRING_MAP:
                outputs[name] = TYPE_STRING_MAP[type_]
            else:
                outputs[name] = type_.__repr__()
        inputs_ = inspect.get_annotations(self.inputs)
        inputs = {}
        for name, type_ in inputs_.items():
            if type_ in TYPE_STRING_MAP:
                inputs[name] = TYPE_STRING_MAP[type_]
            else:
                inputs[name] = type_.__repr__()
        return {"helpstring":docstring, "params":all_annotations, "defaults":all_defaults, "inputs":inputs, "outputs":outputs}
    
    
    def __call__(self):
        #If an atom which runs AFTER this atom is marked load from cache,
        #then we can safely skip this atom's execution
        if self.cache_behavior == CACHE_SKIP:
            self.info(f"Skipping {self.task_name} due to cache settings")  
            pass
        else:
            self.pre_task()
            self.task_body()
            self.post_task()
            self.return_values["restricted"] = self.restricted
            return self.return_values

    def __repr__(self):
        return f"<FlowAtom: {self.task_name}>"
    


    
    
    