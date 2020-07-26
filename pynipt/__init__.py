# Python NeuroImage Pipeline Tool
from .lib import plugin
from .lib.bucket import Bucket
from .lib.interface import InterfaceBuilder
from .lib.pipeline import PipelineBuilder, Pipeline
from .lib.processor import Processor

from .config import config
from .utils import get_list_addons
import importlib
import sys

__version__ = '0.2.3'
__all__ = ['Bucket',
           'Processor',
           'InterfaceBuilder',
           'PipelineBuilder',
           'Pipeline',
           'load']

# mapping Pipeline to load
load = Pipeline

if len(get_list_addons()):
    # add add-in modules if any
    for name, module in get_list_addons().items():
        setattr(sys.modules[__name__], name, importlib.import_module(module))
        __all__.append(name)
