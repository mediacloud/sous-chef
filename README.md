# Sous-Chef

UNDER CONSTRUCTION! 

A package which wraps prefect up in a little easily configurable bow, for self-validating and freely configurable data pipelines.

We call a pipeline configuration a "recipe". This is a YAML file which specifies a set of atoms and connections between them. 

#### Two Entrypoints:

1. By Directory- point to a directory with a recipe.yaml and optionally a mixins.yaml file
- `python run_recipe.py -d ../path/to/recipe/directory/`

2. By Directory with Date Iteration- modify start_date and end_date to 
- `python run_recipe.py -d ../path/to/recipe/directory/ -s start_date(%Y-%m-%d)`

#### Recipes

All of the recipes I've been writing for this tool live at a [different, private repository](https://github.com/mediacloud/sous-chef-recipes)

the 'tests' folder there contains recipes which demonstrate the basic shape and functionality of the tool 

The [Atom Wishlist](Atom_Wishlist.md) is where I am storing the list of new components I'll be adding as time moves on.

### Version History

__v1.0__ - Upgrade to prefect 3
__v0.1__ - First beta tag for versioning


