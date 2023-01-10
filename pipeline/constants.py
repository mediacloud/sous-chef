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





