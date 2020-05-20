# Python NeuroImage Pipeline Tool
from .lib import plugin
from .lib.bucket import Bucket
from .lib.interface import InterfaceBuilder
from .lib.pipeline import PipelineBuilder, Pipeline
from .lib.processor import Processor

from .config import config
from paralexe import Scheduler

__version__ = '0.1.1'
__all__ = ['Bucket',
           'Processor',
           'InterfaceBuilder',
           'PipelineBuilder',
           'Scheduler',
           'Pipeline']
