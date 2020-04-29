import os
import shutil
from .utils import intensive_mkdir
from datetime import date
import configparser


#%% Set config
def create_config_file(cfg, path):
    # Config for Pandas Dataframe
    cfg['Display'] = dict(Max_Row='100',
                          Max_Colwidth='100')

    # Config for Dataset structure
    cfg['Dataset structure'] = dict(dataset_path='Data',
                                    working_path='Processing',
                                    results_path='Results',
                                    masking_path='Mask',
                                    temporary_path='Temp',
                                    ignore='.DS_Store')

    # Config for Plugin
    __plugin_path = os.path.join(os.path.expanduser("~"), '.pynipt', 'plugin')
    cfg['Plugin'] = dict(plugin_path=__plugin_path,
                         interface_plugin_path=os.path.join(__plugin_path, 'interface_default.py'),
                         pipeline_plugin_path=os.path.join(__plugin_path, 'pipeline_default.py'))

    # Computing and processing related
    cfg['Preferences'] = dict(timeout='10',
                              daemon_refresh_rate='0.1',
                              number_of_thread='4',
                              verbose='yes',
                              logging='yes',
                              )

    with open(path, 'w') as configfile:
        cfg.write(configfile)


def restore_config():
    cfg_path = os.path.join(os.path.expanduser("~"), '.pynipt', 'pyniptrc')
    if os.path.exists(cfg_path):
        dirname, filename = os.path.split(cfg_path)
        shutil.copy(cfg_path, os.path.join(dirname, '{}_{}'.format(filename, date.today().strftime("%y%m%d"))))
        os.unlink(cfg_path)
    config = configparser.RawConfigParser()
    create_config_file(config, cfg_path)


#%% Load config
cfg_path = os.path.join(os.path.expanduser("~"), '.pynipt', 'pyniptrc')
config = configparser.ConfigParser()

if not os.path.exists(cfg_path):
    intensive_mkdir(os.path.dirname(cfg_path))
    create_config_file(config, cfg_path)

config.read(cfg_path)


if __name__ == '__main__':
    pass