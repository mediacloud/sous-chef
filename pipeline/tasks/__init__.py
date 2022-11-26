import pkgutil
#This allows all modules in this directory to be loaded automatically with 
#from tasks import *
__path__ = pkgutil.extend_path(__path__, __name__)
for importer, modname, ispkg in pkgutil.walk_packages(path=__path__, prefix=__name__+'.'):
        __import__(modname)