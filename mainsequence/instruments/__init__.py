from .instruments import *



from mainsequence.client import Constant as _C


# -----------------------------
# Gurantee constants on any workflows
# -----------------------------

constants_to_seed = dict(
    REFERENCE_RATE__TIIE_28="TIIE_28",
    REFERENCE_RATE__TIIE_91="TIIE_91",
    REFERENCE_RATE__TIIE_182="TIIE_182",
    REFERENCE_RATE__TIIE_OVERNIGHT="TIIE_OVERNIGHT",

    REFERENCE_RATE__CETE_28="CETE_28",
    REFERENCE_RATE__CETE_91="CETE_91",
    REFERENCE_RATE__CETE_182="CETE_182",
    REFERENCE_RATE__TIIE_OVERNIGHT_BONDES="TIIE_OVERNIGHT_BONDES",



    #curves
    ZERO_CURVE__BANXICO_M_BONOS_OTR = "BANXICO_M_BONOS_OTR",
    ZERO_CURVE__VALMER_TIIE_28   = "F_TIIE_28_VALMER",
    ZERO_CURVE__POLYGON_UST_CMT_ZERO_CURVE_UID="POLYGON_UST_CMT_ZERO_CURVE",
    FIXING_RATES_1D_TABLE_NAME="fixing_rates_1d"
)

_C.create_constants_if_not_exist(constants_to_seed)