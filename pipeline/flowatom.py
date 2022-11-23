import inspect
from pydantic import BaseModel
from prefect import flow

"""
This is the magic class which performs most of the mucking about with python innards
in order to specify a nice encapsulated and validatable confuguration vocabulary
"""

DEFAULTS = "_defaults"

class FlowAtom(object):
    #Silly singleton pattern lets us register subclasses to this parameter
    _REGISTERED_ATOMS = {}

    task_name:str
    _defaults:{"task_name":"A placeholder name"}
    
    def __init__(self, params):
        self.__validate_and_apply(params)
    
    #Easy Access to the subclass registry 
    @classmethod
    def get_atoms(cls):
        return cls._REGISTERED_ATOMS
    
    #This takes care of registering the atom locally for our pipeline project,
    #and also running the prefect registration hook. 
    @classmethod
    def register(cls, name):
        
        def _register(stepclass):
            cls._REGISTERED_ATOMS[name] = flow(stepclass, name=name)
            return stepclass 
        return _register
    

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
                else:
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
            
        
  
    def task_body(self):
        raise RunTimeError("task_body is Unimplimented")
    
    def __pre_task(self):
        #Could be used to do data interface configuration, for example
        pass 
        
    def __post_task(self):
        #Could be used to do environment teardown, for example
        pass
    
    def __call__(self):
        self.__pre_task()
        self.task_body()
        self.__post_task()
        

    
    