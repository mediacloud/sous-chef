import inspect
from pydantic import BaseModel
from prefect import flow, task
from .datastrategy import DataStrategy
from .constants import DATA, DATASTRATEGY, NOSTRAT, DEFAULTS, CACHE_STEP, CACHE_SKIP, CACHE_LOAD, CACHE_SAVE, STRING_TYPE_MAP
"""
This is the magic class which performs most of the mucking about with python innards
in order to specify a nice encapsulated and validatable confuguration vocabulary
"""


class FlowAtom(object):
    #Silly singleton pattern lets us register subclasses to this parameter
    _REGISTERED_ATOMS = {}

    task_name:str
    _defaults:{"task_name":"A placeholder name"}
    _new_document:False
    
    def __init__(self, params, data_config, document=False):
        if not document:
            self.task_inputs = inspect.get_annotations(self.inputs)
            self.task_outputs = inspect.get_annotations(self.outputs)
        
            self.__setup_strategy(data_config)
            self.__validate_and_apply(params)
        
            self.setup_hook(params, data_config)
        else:
            print("THIS IS ONLY USED FOR OFFLINE DOCUMENTATION GENERATION")
            self.docs = self.document()
            


    
    #Easy Access to the subclass registry 
    @classmethod
    def get_atoms(cls):
        return cls._REGISTERED_ATOMS
    
    #This takes care of registering the atom locally for our pipeline project,
    #and also running the prefect registration hook. 
    @classmethod
    def register(cls, name):
        
        def _register(stepclass):
            cls._REGISTERED_ATOMS[name] = task(stepclass, name=f"setup {name}")
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
                raise RuntimeError(f"Bad Configuration, {key} is not a valid configuration key. Options are: {all_annotations.keys()}")
            if not isinstance(value, all_annotations[key]):
                raise RuntimeError(f"Bad Configuration, {key} must be {all_annotations[key]}")
            
            set_params.append(key)    
            setattr(self, key, value)
        
        #Try to apply defaults for parameters which are unconfigured
        for key, value in all_annotations.items():
            if key not in set_params:
                if key not in all_defaults:
                    raise RuntimeError(f"Bad Configuration, missing required parameter {key}:{value}")
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
            raise RuntimeError(f"Bad Configuration: {strat_name} is not a valid data strategy")
            
        self.cache_behavior = data_config[CACHE_STEP]
            
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
        if self.__data_strategy.inputs is not None:
            if self.cache_behavior == CACHE_LOAD:
                self.data, self.results = self.get_data(cache=True)
            else:
                self.data, self.results = self.get_data()
        
    #Task finish- write self.data to the dataframe
    def post_task(self):
        if self.__data_strategy.outputs is not None:
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
            pass
        else:
            self.pre_task()
            self.task_body()
            self.post_task()
    


    
    
    