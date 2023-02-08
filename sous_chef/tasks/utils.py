import importlib.util
import sys


#Defers importing a module until its code is actually executed.
#This significantly lowers the potential memory overhead, as more pipeline atoms get implimented. 
def lazy_import(name):
    spec = importlib.util.find_spec(name)
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module