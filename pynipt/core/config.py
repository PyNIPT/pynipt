import sys
import os
import shutil
from ..utils import intensive_mkdir
from datetime import date
if sys.version_info[0] == 3:
    import configparser
else:
    import ConfigParser as configparser

#%% Set config
def create_config_file(cfg, path):

    # Config for Pandas Dataframe
    cfg.add_section('Display')
    cfg.set('Display', 'Max_Row', 100)
    cfg.set('Display', 'Max_Colwidth', 100)

    # Config for Dataset structure
    cfg.add_section('Dataset structure')
    cfg.set('Dataset structure', 'dataset_path', 'Data')
    cfg.set('Dataset structure', 'working_path', 'Processing')
    cfg.set('Dataset structure', 'results_path', 'Results')
    cfg.set('Dataset structure', 'masking_path', 'Mask')
    cfg.set('Dataset structure', 'temporary_path', 'Temp')
    cfg.set('Dataset structure', 'ignore', '.DS_Store')

    # Config for Plugin
    cfg.add_section('Plugin')
    __plugin_path = os.path.join(os.path.expanduser("~"), '.pynipt', 'plugin')
    cfg.set('Plugin', 'plugin_path', __plugin_path)
    cfg.set('Plugin', 'interface_plugin_path', os.path.join(__plugin_path, 'interface_default.py'))
    cfg.set('Plugin', 'pipeline_plugin_path', os.path.join(__plugin_path, 'pipeline_default.py'))

    # Computing and processing related
    cfg.add_section('Preferences')
    cfg.set('Preferences', 'daemon_refresh_rate', '0.1')
    cfg.set('Preferences', 'number_of_thread', '4')
    cfg.set('Preferences', 'verbose', True)
    cfg.set('Preferences', 'logging', True)

    with open(path, 'w') as configfile:
        cfg.write(configfile)


def restore_config():
    # cfg_path = os.path.join(os.path.expanduser("~"), '.pyniptrc')
    cfg_path = os.path.join(os.path.expanduser("~"), '.pynipt', 'pyniptrc')
    if os.path.exists(cfg_path):
        dirname, filename = os.path.split(cfg_path)
        shutil.copy(cfg_path, os.path.join(dirname, '{}_{}'.format(filename, date.today().strftime("%y%m%d"))))
        os.unlink(cfg_path)
    config = configparser.RawConfigParser()
    create_config_file(config, cfg_path)


#%% Load config
cfg_path = os.path.join(os.path.expanduser("~"), '.pynipt', 'pyniptrc')
config = configparser.RawConfigParser()

if not os.path.exists(cfg_path):
    intensive_mkdir(os.path.dirname(cfg_path))
    create_config_file(config, cfg_path)

config.read(cfg_path)