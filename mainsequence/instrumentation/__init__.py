from .utils import *

if "tracer" not in locals():
    tracer_instrumentator = TracerInstrumentator()
    tracer = tracer_instrumentator.build_tracer()
