import pipeline
from pipeline.constants import STRING_TYPE_MAP
from pprint import pprint
import yaml

doc_data = pipeline.get_documentation()
pprint(doc_data)

stream = open('task_documentation.yaml', 'w')
yaml.dump(doc_data, stream)