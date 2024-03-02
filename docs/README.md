### Under The Hood:

#### Pipeline (in `__init__`)
Main Pipeline Author Class- validates and runs the pipeline configuration

#### FlowAtom
Parent Class for each step of the flow process. Impliments atom-level configuration validation and data interface things. 
Subclasses live in tasks/ and are registered to the parent singleton. Exposes a really tidy syntax for each task step. 

#### DataStrategy
Manages actually loading and writing data. 
Subclassed for different kinds of data interfaces, also registered to a parent singleton.
PandasStrategy is a good default right now- it creates a pandas dataframe and saves it as a CSV in between steps. FlowAtom Access to input and output columns is managed under the hood. 
 

### Smaller fixes:
- The CSV Input task is uselessly finnicky, since we need to use the csv header names throughout a recipe. A fix would let us give new internal names. 

### Longer term plan:
- Task name normalization
- Better documentation, which eventually builds out into a UI for recipe and menu authoring.