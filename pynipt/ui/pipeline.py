import sys
from ..core.base import config as __config
from .bucket import Bucket

from IPython import get_ipython
try:
    from ipywidgets import widgets
except:
    pass

notebook_env = False

if get_ipython() and len(get_ipython().config.keys()):
    from tqdm import tqdm_notebook as progressbar
    from ipywidgets.widgets import HTML as HTML
    from IPython.display import display
    notebook_env = True

else:
    from pprint import pprint as display
    from tqdm import tqdm as progressbar

    def HTML(message): return message

    def clear_output(): pass

def display_html(message):
    return display(HTML(message))

plugin_path = __config.get('Plugin', 'pipeline_plugin_path')
__default_interface_path = __config.get('Plugin', 'interface_plugin_path')
__default_pipeline_path = __config.get('Plugin', 'pipeline_plugin_path')


def plugin_interface(path):
    if sys.version_info[0] == 3:
        from importlib.machinery import SourceFileLoader as __load_source
        return __load_source('Interface', path).load_module().Interface
    else:
        from imp import load_source as __load_source
        return __load_source('Interface', path).Interface


def plugin_pipeline(path):
    if sys.version_info[0] == 3:
        from importlib.machinery import SourceFileLoader as __load_source
        return __load_source('pipeline', path).load_module()
    else:
        from imp import load_source as __load_source
        return __load_source('pipeline', path)


Interface = plugin_interface(__default_interface_path)
pipelines = plugin_pipeline(__default_pipeline_path)


class Pipeline(object):
    """ Pipeline handler

    This class is the major features of PyNIT project (for most of general users)
    You can either use default pipeline packages we provide or load custom designed pipelines
    """
    def __init__(self, path, logger=True, **kwargs):
        """Initiate class

        :param path:    dataset path
        :param logger:  generate log file (default=True)
        :type path:     str
        :type logger:   bool
        """

        # Define default attributes
        self._bucket = Bucket(path, **kwargs)
        self._msi = self._bucket.msi
        self._interface = None
        self._n_threads = None

        self._pipeobj = pipelines
        self._logger = logger
        self.detach_package()

        if 'n_threads' in kwargs.keys():
            self._n_threads = kwargs['n_threads']
        else:
            from .processor import default_n_threads
            self._n_threads = default_n_threads

        # Print out project summary
        print(self._bucket.summary)

        # Print out installed (available) pipeline packages
        avails = ["\t{} : {}".format(*item) for item in self.installed_packages.items()]
        output = ["\nList of installed pipeline packages:"] + avails
        print("\n".join(output))

    def detach_package(self):
        """ Detach selected pipeline package
        """
        self.selected = None
        self._stored_id = None

    @property
    def installed_packages(self):
        pipes = [pipe for pipe in dir(self._pipeobj) if '__' not in pipe if pipe[0] != '_'
                 if 'PipelineBuilder' not in pipe]
        n_pipe = len(pipes)
        list_of_pipes = dict(zip(range(n_pipe), pipes))
        return list_of_pipes

    def select_package(self, package_id, verbose=False, listing=True, **kwargs):
        """Initiate package

        :param package_id:  Id code for package to initiate
        :param verbose:     Printing out the help of initiating package
        :param kwargs:      Input parameters for initiating package
        :param tag:         Tag on package folder
        :type package_id:   int
        :type verbose:      bool
        :type kwargs:       key=value pairs
        """
        self._bucket.update()

        # convert package ID to package name
        if isinstance(package_id, int):
            self._stored_id = package_id
            package_id = self.installed_packages[package_id]

        if package_id in self.installed_packages.values():
            self._interface = Interface(self._bucket, package_id, logger=True, n_threads=self._n_threads)
            command = 'self.selected = self._pipeobj.{}(self._interface, self._n_threads'.format(package_id)
            if kwargs:
                command += ', **{})'.format('kwargs')
            else:
                command += ')'
            exec(command)

        else:
            raise Exception

        if verbose:
            print(self.selected.__init__.__doc__)

        if listing: # TODO: If listing is True
            print("The pipeline package '{}' is selected.\n"
                  "Please double check if all parameters are "
                  "correctly provided before run this pipline".format(package_id))
            avails = ["\t{} : {}".format(*item) for item in self.selected.installed_packages.items()]
            output = ["List of available pipelines in selected package:"] + avails
            print("\n".join(output))

    def check_progression(self):
        if self.selected is not None:
            param = self.selected.interface.scheduler_param
            total = len(param['queue']) + len(param['done'])
            display_html('Number of threads: {}'.format(param['n_threads']))
            return progressbar(total=total,
                               desc=self.installed_packages[self._stored_id],
                               initial=len(param['done']))

    def set_param(self, **kwargs):
        """Set parameters

        :param kwargs:      Input parameters for current initiated package
        """
        if self.selected:
            for key, value in kwargs.items():
                if hasattr(self.selected, key):
                    setattr(self.selected, key, value)
                else:
                    raise Exception
        else:
            raise Exception

    def get_param(self):
        if self.selected:
            # defalt pipeline method: installed_packages, interface
            return dict([(param, getattr(self.selected, param)) for param in dir(self.selected) if param[0] != '_'
                         if 'pipe_' not in param if param not in ['installed_packages', 'interface']])
        else:
            return None

    def howto(self, idx):
        """ Print help document for package

        :param idx: index of available pipeline package
        :type idx: int
        :return:
        """
        if isinstance(idx, int):
            idx = self.installed_packages[idx]
        if idx in self.installed_packages.values():
            command = 'print(self._pipeobj.{}.__init__.__doc__)'.format(idx)
            exec(command)

    def run(self, idx, **kwargs):
        """Execute selected pipeline

        :param idx: index of available pipeline
        :type idx: int
        :return:
        """
        self.set_param(**kwargs)
        exec('self.selected.pipe_{}()'.format(self.selected.installed_packages[idx]))

    def get_interface_object(self):
        if self._interface:
            return self._interface
        else:
            raise Exception

    def get_bucket_object(self):
        return self._bucket

