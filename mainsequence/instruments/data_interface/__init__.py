from .data_interface import DateInfo, MockDataInterface, MSInterface
from mainsequence.client import Constant as _C

import os

def _make_backend():
    backend = os.getenv("MSI_DATA_BACKEND", "mainsequence").lower()
    return MSInterface() if backend == "mainsequence" else MockDataInterface()

# export a single, uniform instance
data_interface = _make_backend()



constants_to_create=dict(


DISCOUNT_CURVES_TABLE         = "discount_curves",
REFERENCE_RATES_FIXING_TABLE  = "fixing_rates_1d",
)

_C.create_constants_if_not_exist(constants_to_create)
