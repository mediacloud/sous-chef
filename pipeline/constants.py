#CONFIGURATION_KEYS
ID = "id"
STEPS = "steps"
PARAMS = "params"
DATASTRATEGY = "data_strategy"
RUNNAME = "name"

#DATA STRATEGY INTERNALS
DATALOCATION = "data_location"
READLOCATION = "read_location"
WRITELOCATION = "write_location"
NEWDOCUMENT = "creates_new_document"
DOCUMENTMAP = "document_map"
USER_CONFIGURED_OUTPUT = "configured__"
USER_CONFIGURED_COLUMNS = "columns"
LOAD_IF_CACHED = "load_if_cached"
CACHE_STEP = "cache_step"
CACHE_SKIP = "cache_skip"
CACHE_SAVE = "cache_save"
CACHE_LOAD = "cache_load"
CACHE_HASHES = "cache_hashes"

#Useful for configured input columns
STRING_TYPE_MAP = {
    "float": float,
    "int": int,
    "str": str
}

#default data strategy
NOSTRAT = "NoStrategy"

#FLOW ATOM INTERNALS
DEFAULTS = "_defaults" #key for default param values
INPUTS = "inputs" #key for functional input mapping 
OUTPUTS = "outputs" #key for functional output mapping
DATA = "_data" #key for data configuration 





