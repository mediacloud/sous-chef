import requests
from typing import List, Dict
from ..flowatom import FlowAtom
from .utils import lazy_import

language = lazy_import("mcproviders.language")

@FlowAtom.register("LanguageDetectionTask")
class LanguageDetection(FlowAtom):
    """
    Use mc-providers language estimator to estimate the language of a piece of text
    """
    
    def inputs(self, text:str):pass
    def outputs(self,language:str):pass
    
    def task_body(self):

        langs = []
        print(self.data.text)
        for text in self.data.text:
            lang = language.top_detected(str(text))
            langs.append(lang)
            
        self.results.language = langs