# Sous-Chef

UNDER CONSTRUCTION! 

A package which wraps prefect up in a little easily configurable bow, for self-validating and freely configurable data pipelines.

We call a pipeline configuration a "recipe". This is a YAML file which specifies a set of atoms and connections between them. 

To invoke a recipe, just run:

` python run_recipe.py ./path/to/recipe.yaml `


#### Recipes
The available atoms right now can be seen in `docs/task_documentation.yaml` - if this is out of date, regenerate it by running `python generate_docs.py` 

All of the recipes I've been writing for this tool live at a [different repository](https://github.com/mediacloud/SousChef-Recipes)

the 'tests' folder there contains recipes which demonstrate the basic shape and functionality of the tool 

The [Atom Wishlist](Atom_Wishlist.md) is where I am storing the list of new components I'll be adding as time moves on.

#### Installation
This is not quite ready for primetime yet, so there's no formal packaging solution. 
For now just clone via github and install requirements.txt. You should be able to run a recipe from the root of this project. 


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
 
 
### Package TODO:
- Real Tests, Good God Please.
- Recipe Variables- to make reuse easier. 
- Multidocument Inputs
- "Menus" - ie, ets of recipes with variables 

### Smaller fixes:
- The CSV Input task is uselessly finnicky, since we need to use the csv header names throughout a recipe. A fix would let us give new internal names. 

### Longer term plan:
- Task name normalization
- Better documentation, which eventually builds out into a UI for recipe and menu authoring.

