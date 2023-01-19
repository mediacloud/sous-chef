import pipeline
from pipeline.constants import STRING_TYPE_MAP
from pprint import pprint
import yaml



class MyDumper(yaml.SafeDumper):
    # HACK: insert blank lines between top-level objects
    # inspired by https://stackoverflow.com/a/44284819/3786245,
    # stolen wholesale from https://github.com/yaml/pyyaml/issues/127#issuecomment-525800484
    def write_line_break(self, data=None):
        super().write_line_break(data)
        if len(self.indents) == 1:
            super().write_line_break()
            super().write_line_break()

def get_yaml_docs():
    #save out a YAML file with all of the task documentation present.
    # - Nothing too fancy.
    doc_data = pipeline.get_documentation()
    stream = open('task_documentation.yaml', 'w')
    yaml.dump(doc_data, stream, Dumper=MyDumper)
    
    
if __name__ == "__main__":
    get_yaml_docs()