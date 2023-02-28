import unittest
from typing import Optional
import pytest
import pandas as pd
import numpy as np


from .. import datacollage


class TestDataCollage(unittest.TestCase):
    
    def test_datacollage_read_write(self):
        A = pd.DataFrame(np.random.randn(6, 4), columns=list("ABCD"))
        B = pd.DataFrame(np.random.randn(8,2), columns=list("EF"))

        docs = {
            "First":A,
            "Second":B
        }
        
        dc = datacollage.DataCollage(docs)
        
        dc["G"] = dc.F + dc.E
        
        
        assert (dc.G[0] == dc.F[0] + dc.E[0])
        