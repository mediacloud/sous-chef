# MC Pipeline Tool 

UNDER CONSTRUCTION! 
We're having fun here!

A package which wraps prefect up in a little easily configurable bow, for self-validating and freely configurable data pipelines

All you need to do to run a pipeline is:

```
from pipeline import RunPipeline

config = {...}

RunPipeline(config)
```

#### Samples
Right now I have two sample pipelines- one with and one without a datastrategy layer. These are here in lieu of tests- all the major features of the package are demonstrated, but there's nothing that really tests the validation components. 

#### Installation
I make no claims about the build! A setup.py exists, but don't rely on a system level installation of this at this point. Samples can be run from the root directory. 



### Classes:

#### Pipeline (in `__init__`)
Main Pipeline Author Class- validates and runs the pipeline configuration

#### FlowAtom
Parent Class for each step of the flow process. Impliments atom-level configuration validation and data interface things. 
Subclasses live in tasks/ and are registered to the parent singleton. Exposes a really tidy syntax for each task step. 

#### DataStrategy
Manages actually loading and writing data. 
Subclassed for different kinds of data interfaces, also registered to a parent singleton.
PandasStrategy is a good default right now- it creates a pandas dataframe and saves it as a CSV in between steps. FlowAtom Access to input and output columns is managed under the hood. 
 
 
### Example

This is an example implementation of a simple flowatom subclass
```python
@FlowAtom.register("DivisibleByNTask")
class DivisibleByNTask(FlowAtom):
    
    n:int
    _defaults:{
        "n": 2
    }
            
    def inputs(self, to_divide:int): pass
    def outputs(self, divisible:bool): pass
    
    def task_body(self):
        output = []
        
        for val in self.data.to_divide:
            output.append(val % self.n == 0)
        
        self.data.divisible = output
```

The following json (from `samples/basic_datastrategy_sample.py`) configures this atom to test divisibility by 5 of values loaded from a column named `factor_count` and to place the result in a column named `5_divisible`. 
```python
...
    {
        "id":"DivisibleByNTask",
        "params":{
            "task_name":"is count divisible by five?",
            "n":5
        }, 
        "inputs":{
            "to_divide":"factor_count"
        },
        "outputs":{
            "divisible":"5_divisible"
        }
    },
...
```


### Package TODO:
- Flow Atoms should be able to return expected parameter types as documentation- this will enable easier config authoring, and eventually will make a hypothetical config authoring interface very straightforward. 
- Real Tests, Good God Please.
- YAML config parsing
- Better Type Validation, Custom Atom Validation for things like datestrings
- Caching or Re-starting? Only really useful for my development I think, but...
- Document tracking. Some atoms extend documents, some create new documents... current pattern only supports one doc per config, which excludes a good % of usecases. There is a way that the datastrategy could handle this detail under the hood, so the user doesn't really have to think about it. I'll have to brainstorm the whole thing a bit before digging in. 



### Questions
1. Extensions vs Transformations. Right now all of the atoms simply extend an original discovery document. If we wanted to, say, sort the most common values from a field, we would end up with a transformation- a totally new document. We could just save this in an additional column of the dataframe, but this could mess things up if you then used this column the wrong way. Is this just an extra kind of type validation, or is some different mechanism required?
3. Should it get a fancy name? I think it should get something better than 'mc_pipeline', but I know that MC's preference is for more straightforward names. 
