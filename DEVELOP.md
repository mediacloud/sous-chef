Developer Reference
===================

The sous-chef stack is three repositories. This core library of data science tasks, the `sous-chef-kitchen` - a deployable api layer- and the `sous-chef-kitchen-frontend`.  This covers just this library. 

A sous-chef flow is just a python function decorated with a `register_flow()` context. 
Broadly speaking this is just a framework for defining flows with wrappers that make them easy to plug-and-play
with the sous-chef-kitchen interface, so that they can be accessed by users via the sous-chef-kitchen-frontend or 
called directly via the sous-chef-kitchen api. 

In general these functions will proceed by downloading a bunch of data from mediacloud, applying some kind of etl 
process to that data, then upload the result to the cloud for users to grab on their own time. The output of a function
is only the 'artifacts' that a user needs to find the output of that code and to understand what occured. Flows are defined in `sous_chef/flow`- and 'registered' to the consumer through inclusion in `sous_chef/flow/__init__.py`

There's a small library of 'tasks', resuable chunks of code decorated with a prefect task() context, that handle the bulk of the actual processing, so the flow function itself can be only responsible for orchestration- that's not a strictly enforced distinction, but so far has continued to seem like best practice to me. These live in `sous_chef/tasks`

Inputs to flows are defined by composable pydantic models found in `sous_chef/params`- there's a small library of often reused input types (like mediacloud queries, for example) so when composing an input schema for a new flow you only need to author inputs for parameters that are unique/novel to your flow. 

Outputs from flows are always **artifacts**, represented as fields on a FlowOutput
model (a subclass of `BaseFlowOutput`) whose fields are `BaseArtifact` instances.
The `BaseArtifact` handles serialization so that it can be stored in a prefect
artifact store ephemerally, or sent as json via webhook etc. These are not raw
data from a flow, rather they are records of the side-effects of the flow (like
an Upload URL or an LLM Cost Summary). I'd say it's unlikely you'll need to author
new artifacts, except in the case of some new external dependency or some newly
desired system behavior. They live in `sous_chef/artifacts`

#### FlowOutput Models

Flow outputs are defined as Pydantic models inheriting from `BaseFlowOutput`.
Each field should be a `BaseArtifact`. This keeps type information close to
the flow, and the kitchen can derive an output schema for API/UX from the model.
See `sous_chef.flow.BaseFlowOutput` for details.


### Inter-stack Dependencies

Except in a few cases, adding new functionality should only involve code-edits to this repository. The version of sous-chef that is deployed via sous-chef-kitchen is pinned via a git tag in `sous-chef-kitchen/pyproject.toml` - so bumping that value is required to catch new features in deployment. 

Both input parameters and output artifacts also define ui hints, so that the sous-chef-frontend interface can define unique ui blocks for them- so if you do extend the existing library of params or artifacts consider also defining an associated frontend component in `sous-chef-kitchen-frontend`

In the case that you do add a new external dependency, you'll likely need to include a secret to use it. secrets.py defines a set of functions that look for secrets as registered in prefect, and then fallback to your local environment if you're running/testing locally. To add a new secret to prefect, refer to `sous-chef-kitchen/docker/prefect-config.py` - the value itself is stored in a private secret store that's loaded during the deploy loop. 