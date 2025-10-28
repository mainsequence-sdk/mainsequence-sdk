import os

from mainsequence.client import Constant as _C

from .data_interface import DateInfo,  MSInterface



# export a single, uniform instance
data_interface = MSInterface() 


constants_to_create = dict(
    DISCOUNT_CURVES_TABLE="discount_curves",
    REFERENCE_RATES_FIXING_TABLE="fixing_rates_1d",
)

_C.create_constants_if_not_exist(constants_to_create)
