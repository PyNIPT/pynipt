import sys
import time
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
    from IPython.display import display
    notebook_env = True

else:
    from pprint import pprint as display
    from tqdm import tqdm as progressbar


plugin_path = __config.get('Plugin', 'pipeline_plugin_path')
_default_interface_path = __config.get('Plugin', 'interface_plugin_path')
_default_pipeline_path = __config.get('Plugin', 'pipeline_plugin_path')
_verbose_option = bool(__config.get('Preferences', 'verbose'))
_logging_option = bool(__config.get('Preferences', 'logging'))

_interface_loaded = False
_pipelines_loaded = False
_plugged_in_pipeline = []


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
        return __load_source('pipelines', path).load_module()
    else:
        from imp import load_source as __load_source
        return __load_source('pipelines', path)


def load_plugin(interface_path=None, pipeline_path=None):
    global Interface
    global pipelines
    global _interface_loaded
    global _pipelines_loaded
    global _plugged_in_pipeline

    if interface_path is None:
        if _interface_loaded is False:
            Interface = plugin_interface(_default_interface_path)
            _interface_loaded  = True
        else:
            if pipeline_path is None:
                Interface = plugin_interface(_default_interface_path)
    else:
        InterfaceBase = plugin_interface(_default_interface_path)
        InterfacePlugin = plugin_interface(interface_path)

        class PlugIn(InterfaceBase, InterfacePlugin):
            def __init__(self, *args, **kwargs):
                super(PlugIn, self).__init__(*args, **kwargs)

        Interface = PlugIn

    if pipeline_path is None:
        if _pipelines_loaded is False:
            pipelines = plugin_pipeline(_default_pipeline_path)
            if len(_plugged_in_pipeline) == 0:
                _plugged_in_pipeline = dir(pipelines)
            _pipelines_loaded = True
        else:
            pipelines = plugin_pipeline(_default_pipeline_path)
            list_of_attr = dir(pipelines)
            for att in list_of_attr:
                if att not in _plugged_in_pipeline:
                    delattr(pipelines, att)
    else:
        pipelines = plugin_pipeline(pipeline_path)


def clear_plugin():
    load_plugin()


class Pipeline(object):
    """ Major user interface to processing pipeline.
    PyNIPT main package does not contain any interface commands or pipeline packages in source code.
    All the interface commands and pipeline packages need to be installed by plugin.

    The default example plugin scripts will be downloaded on your configuration folder
    (under .pynipt/plugin in user's home directory)

    Examples:
        Usage example to select pipeline

        Import module and initiate pipeline object
        >>> import pynipt as pn
        >>> pipe = pn.Pipeline('/project/dataset/path')
        The installed pipeline plugin will be listed here

        >>> pipe.howto(0)       # print help for the 0th pipeline package if any
        The help document will be printed here if the verbose option is True in user's config file

        Select 0th pipeline package
        >>> pipe.select_package(0)
        The available pipelines in the package will be listed here if the verbose option is True in user's config file

        Run 0th pipeline in selected package
        >>> pipe.run(0)
        The description of the pipeline will be printed here if the verbose option is True in user's config file

        Check the progression bar of running pipeline
        >>> pipe.check_progression()

    You can either use default pipeline packages we provide or load custom designed pipelines
    """
    def __init__(self, path, **kwargs):
        """Initiate class

        :param path:    dataset path
        :param logger:  generate log file (default=True)
        :type path:     str
        :type logger:   bool
        """

        # Define default attributes
        self._bucket = Bucket(path, **kwargs)
        self._msi = self._bucket.msi
        self._interface = None                  # place holder for interface plugin
        self._n_threads = None                  # place holder to provide into Interface class

        self._pipeobj = pipelines               # pipeline plugin will be attached to here
        self.detach_package()

        self._progressbar = None                # to store tqdm object

        if 'n_threads' in kwargs.keys():
            self._n_threads = kwargs['n_threads']
        else:
            from .processor import default_n_threads        # default is saved on config file
            self._n_threads = default_n_threads
        if 'logging' in kwargs.keys():
            self._logger = kwargs['logging']
        else:
            self._logger = _logging_option

        if _verbose_option is True:
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

    def init_pipeline(self, title):
        """Initiate package with given title

        Args:
            title:
        """
        self._bucket.update()
        self._interface = Interface(self._bucket, title,
                                    logger=self._logger,
                                    n_threads=self._n_threads)
        self._pipeline_title = title
        if _verbose_option is True:
            print('Pipeline [{}] is initiated without selecting pipeline package.'.format(title))

    def select_package(self, package_id, **kwargs):
        """Initiate package

        :param package_id:  Id code for package to initiate
        :param kwargs:      Input parameters for initiating package
        :param tag:         Tag on package folder
        :type package_id:   int
        :type kwargs:       key=value pairs
        """
        self._bucket.update()

        # convert package ID to package name
        if isinstance(package_id, int):
            self._stored_id = package_id
            self._pipeline_title = self.installed_packages[package_id]
        else:
            raise Exception

        self._interface = Interface(self._bucket, self._pipeline_title,
                                    logger=self._logger,
                                    n_threads=self._n_threads)
        command = 'self.selected = self._pipeobj.{}(self._interface'.format(self._pipeline_title)
        if kwargs:
            command += ', **{})'.format('kwargs')
        else:
            command += ')'
        exec(command)

        if _verbose_option is True:
            print('Description about this package:\n')
            print(self.selected.__init__.__doc__)
            print("The pipeline package '{}' is selected.\n"
                  "Please double check if all parameters are "
                  "correctly provided before run this pipline".format(self._pipeline_title))
            avails = ["\t{} : {}".format(*item) for item in self.selected.installed_pipelines.items()]
            output = ["List of available pipelines in selected package:"] + avails
            print("\n".join(output))

    def check_progression(self):
        if self._interface is not None:
            param = self._interface.scheduler_param
            queued_jobs = len(param['queue'])
            finished_jobs = len(param['done'])
            self._progressbar =  progressbar(total=queued_jobs + finished_jobs,
                                             desc=self.installed_packages[self._stored_id],
                                             initial=finished_jobs)

            def workon(n_queued, n_finished):
                while n_finished < n_queued + n_finished:
                    delta = n_queued - len(param['queue'])
                    if delta > 0:
                        n_queued -= delta
                        n_finished += delta
                        self._progressbar.update(delta)
                    time.sleep(0.2)
                self._progressbar.close()

            import threading
            thread = threading.Thread(target=workon, args=(queued_jobs, finished_jobs))
            if notebook_env is True:
                display(self._progressbar)
                thread.start()
            else:
                thread.start()

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

        Args:
            idx(int):       index of available pipeline package
        """
        if isinstance(idx, int):
            idx = self.installed_packages[idx]
        if idx in self.installed_packages.values():
            command = 'print(self._pipeobj.{}.__init__.__doc__)'.format(idx)
            exec(command)

    def run(self, idx, **kwargs):
        """ Execute selected pipeline

        Args:
            idx(int):       index of available pipeline package
            **kwargs:       key:value pairs of parameters for this pipeline
        """
        self.set_param(**kwargs)
        if _verbose_option is True:
            exec('print(self.selected.pipe_{}.__doc__)'.format(self.selected.installed_pipelines[idx]))
        exec('self.selected.pipe_{}()'.format(self.selected.installed_pipelines[idx]))

    @property
    def bucket(self):
        return self._bucket

    @property
    def interface(self):
        return self._interface

    def get_workers(self, step_code):
        queues = self._interface.running_obj[step_code].threads.queues
        if queues is not None:
            if len(queues.keys()) > 1:
                return queues
            else:
                return queues[queues.keys()[0]]
        else:
            return None

    def get_builder(self):
        if self.interface is not None:
            from .builder import InterfaceBuilder
            return InterfaceBuilder(self.interface)
        else:
            return None

    def get_dset(self, step_code, ext='nii.gz', regex=None):
        if self.interface is not None:
            proc = self.interface
            proc.update()
            filter = dict(pipelines=proc.label,
                          ext=ext)
            if regex is not None:
                filter['regex'] = regex
            step = proc._get_step_dir(step_code)
            if step_code in proc._executed.keys():
                dataclass = 1
                filter['steps'] = step
            elif step_code in proc._reported.keys():
                dataclass = 2
                filter['reports'] = step
            elif step_code in proc._masked.keys():
                dataclass = 3
                filter['datatypes'] = step
            else:
                return None
            return self.bucket(dataclass, copy=True, **filter)
        else:
            return None


