from prefect import flow


class FlowAtom(object):
    _REGISTERED_ATOMS = {}
    
    def __init__(self, params):
        self.params = params
    
    @classmethod
    def get_atoms(cls):
        return cls._REGISTERED_ATOMS
    
    #This takes care of registering the atom locally for our pipeline project,
    #and also running the prefect registration hook. 
    #Eventually might need to break out more of rest of the prefect functionality but leave as is for now
    @classmethod
    def register(cls, name):
        def _register(stepclass):
            cls._REGISTERED_ATOMS[name] = flow(stepclass, name=name)
            return stepclass 
        return _register
    
    def __validate(self):
        #Soooo I think this will be the most annoying thing to implement 
        #Should we have a call in 
        pass
    
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
    