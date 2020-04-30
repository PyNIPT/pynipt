import os
import shutil
from datetime import date
import configparser
if os.name == 'nt':
    config_file = 'pynipt.ini'
else:
    config_file = '.pyniptrc'


#%% Set config
def create_config_file(cfg, path):
    # Config for pandas.DataFrame
    cfg['Display'] = dict(Max_Row='100',
                          Max_Colwidth='100')

    # Config for Dataset structure
    cfg['Dataset structure'] = dict(dataset_path='Data',
                                    working_path='Processing',
                                    results_path='Results',
                                    masking_path='Mask',
                                    temporary_path='Temp',
                                    ignore='.DS_Store')

    # Computing and processing related
    cfg['Preferences'] = dict(timeout='10',
                              daemon_refresh_rate='0.1',
                              number_of_threads='4',
                              verbose='yes',
                              logging='yes',
                              )

    with open(path, 'w') as configfile:
        cfg.write(configfile)


def restore_config():
    global cfg_path
    if os.path.exists(cfg_path):
        dirname, filename = os.path.split(cfg_path)
        shutil.copy(cfg_path, os.path.join(dirname, '{}_{}'.format(filename, date.today().strftime("%y%m%d"))))
        os.unlink(cfg_path)
    new_config = configparser.RawConfigParser()
    create_config_file(new_config, cfg_path)


#%% Load config
cfg_path = os.path.join(os.path.expanduser("~"), config_file)
config = configparser.ConfigParser()

if not os.path.exists(cfg_path):
    create_config_file(config, cfg_path)
config.read(cfg_path)

if __name__ == '__main__':
    pass
