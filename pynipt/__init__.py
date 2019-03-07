import os
import sys
from .ui import Bucket
from .ui import InterfaceBuilder
from .ui import PipelineBuilder
from .ui import Processor

from .core.config import cfg_path as __cfg_path, \
    create_config_file as __make_config, \
    restore_config
from .core.base import dc as __dc, \
    config as __config
from .utils import intensive_mkdir as __mkdir
from paralexe import Scheduler

# Python NeuroImage Pipeline Tool

if sys.version_info[0] == 3:
    from urllib.request import urlopen as __urlopen
else:
    from urllib2 import urlopen as __urlopen

__version__ = '0.0.1a3'

# URLs for developer plugin modules
__inter_plugin_url = 'https://gist.githubusercontent.com/dvm-shlee/' \
                    '52aa93427b98d1d7099d3736c78bfeb4/raw/' \
                    'interface_default.py'
__pipe_plugin_url = 'https://gist.githubusercontent.com/dvm-shlee/' \
                    '52aa93427b98d1d7099d3736c78bfeb4/raw/' \
                    'pipeline_default.py'

__plugin_path = __config.get('Plugin', 'plugin_path')
__default_interface_path = __config.get('Plugin', 'interface_plugin_path')
__default_pipeline_path = __config.get('Plugin', 'pipeline_plugin_path')


# download default interface from developer's gist
def __download_plugin(url, path):
    if os.path.exists(path):
        os.unlink(path)
    plugin = __urlopen(url)
    with open(path, 'a') as f:
        for line in plugin.readlines():
            f.write(line.decode('ascii'))


def update_default_plugin(module='all'):
    """update default plugin from developer Gist 
    
    Args:
        module: ['all', 'interface', 'pipeline'] 
    """
    if module == 'all':
        __download_plugin(__inter_plugin_url, __default_interface_path)
        __download_plugin(__pipe_plugin_url, __default_pipeline_path)
    elif module == 'interface':
        __download_plugin(__inter_plugin_url, __default_interface_path)
    elif module == 'pipeline':
        __download_plugin(__pipe_plugin_url, __default_pipeline_path)
    else:
        raise ModuleNotFoundError

    # remove cache files
    if sys.version_info[0] == 3:
        import shutil
        if os.path.exists(os.path.join(__plugin_path, '__pycache__')):
            shutil.rmtree(os.path.join(__plugin_path, '__pycache__'))
    else:
        if os.path.exists('{}c'.format(__default_interface_path)):
            os.unlink('{}c'.format(__default_interface_path))
        if os.path.exists('{}c'.format(__default_pipeline_path)):
            os.unlink('{}c'.format(__default_pipeline_path))


# create plugin folder if it does not exist.
if not os.path.exists(__plugin_path):
    __mkdir(__plugin_path)

if not os.path.exists(__default_interface_path):
    __download_plugin(__inter_plugin_url, __default_interface_path)

# download default pipeline from developer's gist
if not os.path.exists(__default_pipeline_path):
    __download_plugin(__pipe_plugin_url, __default_pipeline_path)

from .ui.pipeline import Pipeline

__all__ = ['Bucket',
           'Processor',
           'InterfaceBuilder',
           'PipelineBuilder',
           'Scheduler',
           'Pipeline']


