from .bucket import Bucket
from .plugin import PluginLoader
from ..config import config
from ..errors import *
from typing import Optional
from shleeh.utils import deprecated_warning
import time

try:
    from IPython import get_ipython
    if get_ipython() and len(get_ipython().config.keys()):
        notebook_env = True
    else:
        notebook_env = False
except ModuleNotFoundError:
    notebook_env = False

if notebook_env:
    from tqdm import tqdm_notebook as progressbar
    from IPython.display import display
else:
    from pprint import pprint as display
    from tqdm import tqdm as progressbar


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
        >>> pipe.set_package(0)
        The available pipelines in the package will be listed here if the verbose option is True in user's config file

        Run 0th pipeline in selected package
        >>> pipe.run(0)
        The description of the pipeline will be printed here if the verbose option is True in user's config file

        Check the progression bar of running pipeline
        >>> pipe.check_progression()

    You can either use default pipeline packages we provide or load custom designed pipelines
    """
    def __init__(self, path: str, **kwargs):
        """Initiate class
        Args:
            path: Project path, the BIDS dataset must be placed in 'Data'
            **kwargs: options for Pipeline instance, please see Notes
        Notes:
            Available kwargs for this class are listed below. The default values are defined at configuration.
            logging (bool): Logging object initiated if the value is True
            n_threads:

        :param path:    dataset path
        :param logger:  generate log file (default=True)
        :type path:     str
        :type logger:   bool
        """
        # public
        self.selected = None

        # private
        self._bucket                = Bucket(path)
        self._msi                   = self._bucket.msi      #
        self._interface_plugins     = None                  # place holder for interface plugin
        self._n_threads             = None                  # place holder to provide into Interface class
        self._pipeline_title        = None                  # place holder for the pipeline title
        self._plugin                = PluginLoader()
        self._pipeobj               = None
        self._stored_id             = None
        self._progressbar           = None                  # place holder for tqdm module

        # config parser
        cfg = config['Preferences']
        self._logger    = kwargs['logging']     if 'logging'    in kwargs.keys() else cfg.getboolean('logging')
        self._n_threads = kwargs['n_threads']   if 'n_threads'  in kwargs.keys() else cfg.getint('number_of_threads')
        self._verbose   = kwargs['verbose']     if 'verbose'    in kwargs.keys() else cfg.getboolean('verbose')

        if self._verbose:
            # Print out project summary
            print(self._bucket.summary)

            # Print out installed (available) pipeline packages
            avails = ["\t{} : {}".format(*item) for item in self.installed_packages.items()]
            output = ["\nList of installed pipeline packages:"] + avails
            print("\n".join(output))

    def detach_package(self):
        """ Detach selected pipeline package """
        self.selected   = None
        self._pipeline_title = None
        self._stored_id = None

    @property
    def installed_interfaces(self):
        """ return all available interface imported from installed plugins """
        parser = dict()
        for name, mod in self._plugin.interface_objs.items():
            parser[name] = mod.avail
        return parser

    @property
    def installed_packages(self):
        """ return all available pipeline packages imported from installed plugins """
        return self._plugin.avail_pkgs

    def set_scratch_package(self, title: str):
        """ set scratch package for developing pipeline script from scratch
        Args:
            title: name of pipeline package
        """
        self._bucket.update()
        self.detach_package()
        self._interface_plugins = self._plugin.get_interfaces()(self._bucket, title,
                                                                logger=self._logger,
                                                                n_threads=self._n_threads)
        self._pipeline_title = title
        if self._verbose is True:
            print(f'The scratch package [{title}] is initiated.')

    @property
    def select_package(self):
        """ for a backward compatibility """
        deprecated_warning('selected_package', 'set_package', future=True)
        return self.set_package

    def set_package(self, package_id: int, **kwargs):
        """set pipeline package from installed plugin
        Args:
            package_id: Id code for package to initiate
            **kwargs: key:value pairs of parameters for this pipeline
        Raises:
            IndexError if the package_id is invalid
        """
        self._bucket.update()

        # convert package ID to package name
        if isinstance(package_id, int):
            self._stored_id = package_id
            self._pipeline_title = self.installed_packages[package_id]
        else:
            raise IndexError('Invalid package id')
        self.reset(**kwargs)

        if self._verbose:
            print('Description about this package:\n')
            print(self.selected.__init__.__doc__)
            print("The pipeline package '{}' is running.\n"
                  "Please make sure the all parameters are valid "
                  "for the running pipeline before execution.".format(self._pipeline_title))
            avails = ["\t{} : {}".format(*item) for item in self.selected.installed_pipelines.items()]
            output = ["List of available pipelines in running package:"] + avails
            print("\n".join(output))

    def import_plugin(self, name: str, file_path: str):
        self._pipeobj.from_file(name, file_path)

    def reset(self, **kwargs):
        """ Reset pipeline instance. All items in queue will be reset. """
        # TODO: Find a way to kill all running thread to stop processing
        if self._pipeline_title is not None:
            self._interface_plugins = self._plugin.get_interfaces()(self._bucket, self._pipeline_title,
                                                                    logger=self._logger,
                                                                    n_threads=self._n_threads)
            if self._stored_id:
                self._pipeobj = self._plugin.get_pkgs(self._stored_id)
            if hasattr(self._pipeobj, self._pipeline_title):
                selected_pkg = getattr(self._pipeobj, self._pipeline_title)
                self.selected = selected_pkg(self._interface_plugins, **kwargs)
        else:
            pass

    def check_progression(self, step_code: str = None):
        """Method that can realtime progression of pipeline execution."""
        if self._interface_plugins is not None:
            if step_code is None:
                param = self._interface_plugins.scheduler_param
                queued_jobs = len(param['queue'])
                finished_jobs = len(param['done'])
                desc = self.installed_packages[self._stored_id] if self._stored_id is not None else self._pipeline_title
                self._progressbar = progressbar(total=queued_jobs + finished_jobs,
                                                desc=desc,
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
            else:
                self.schedulers[step_code].check_progress()

    def set_param(self, **kwargs):
        """Set parameters
        Args:
            **kwargs: key:value pairs of parameters for this pipeline
        """
        if self.selected:
            for key, value in kwargs.items():
                if hasattr(self.selected, key):
                    setattr(self.selected, key, value)
                else:
                    raise KeyError(f'[{key}] is invalid key for this pipeline.')
        else:
            raise InvalidApproach('Pipeline package must be set prior.')

    def get_param(self):
        if self.selected:
            plugin_name = self.selected.__module__.split('.')[0]
            return dict([(param, getattr(self.selected, param)) for param in
                         self._plugin.pipeline_objs[plugin_name].arguments if param not in ['interface']])
        else:
            return None

    def howto(self, pkg: int or str):
        """ Print help document for package
        Args:
            pkg: index or full name of pipeline package shown in available packages
        """
        if isinstance(pkg, int):
            pkg = self.installed_packages[pkg]
        if pkg in self.installed_packages.values():
            print(getattr(self._pipeobj, pkg).__init__.__doc__)

    def run(self, idx: int, **kwargs):
        """ Execute selected pipeline
        Args:
            idx(int): index of available pipeline package
            **kwargs: key:value pairs of parameters for this pipeline
        """
        self.set_param(**kwargs)
        selected_pipeline = getattr(self.selected, f'pipe_{self.selected.installed_pipelines[idx]}')
        if self._verbose:
            print(selected_pipeline.__doc__)
        selected_pipeline()

    @property
    def bucket(self):
        return self._bucket

    def remove(self, step_code, mode='processing'):
        """ Remove specified step from the file system
        Args:
            step_code:  the step code you want to remove from file system
            mode:       the data class mode that the step you specified is locating.
        Raises:
            InvalidStepCode
        """
        if isinstance(step_code, list):
            for s in step_code:
                self.interface.destroy_step(s, mode=mode)
        elif isinstance(step_code, str) and (len(step_code) == 3):
            self.interface.destroy_step(step_code, mode=mode)
        else:
            raise InvalidStepCode

    @property
    def interface(self):
        """ interface object that you can access all installed interface functions """
        return self._interface_plugins

    @property
    def schedulers(self):
        """ the namespace to access scheduler of the running step """
        steps = self.builders.keys()
        return {s : self.builders[s].threads for s in steps}

    @property
    def builders(self):
        """ the namespace to access interface builder of the running step """
        return self._interface_plugins.running_obj

    @property
    def managers(self):
        """ the namespace to access manager of the running step """
        steps = self.builders.keys()
        return {s: self.builders[s].mngs for s in steps}

    def get_builder(self):
        if self.interface is not None:
            from .interface import InterfaceBuilder
            return InterfaceBuilder(self.interface)
        else:
            return None

    def get_dset(self, step_code: str,
                 ext: str = 'nii.gz',
                 regex: Optional[str] = None) -> (Bucket, None):
        """ the method to access dataset in specified step
        Notes:
            if you specify the data type on dataset, instead of step code, it will parse the dataset
            from raw dataset folder
        Args:
            step_code:  if the input is datatype instead of step code, return rawdata bucket
            ext:        file extension filter
            regex:      regex pattern for filtering the dataset
        Returns:
            dataset:    pynipt.Bucket object that containing filtered data
        """
        if self.interface is not None:
            proc = self.interface
            proc.update()
            filter_ = dict(pipelines=proc.label,
                           ext=ext)
            if regex is not None:
                filter_['regex'] = regex
            try:
                step = proc.get_step_dir(step_code)
            except KeyError:
                try:
                    step = proc.get_report_dir(step_code)
                except KeyError:
                    try:
                        step = proc.get_mask_dir(step_code)
                    except KeyError:
                        step = step_code
            if step_code in proc.executed.keys():
                dataclass = 1
                filter_['steps'] = step
            elif step_code in proc.reported.keys():
                dataclass = 2
                filter_['reports'] = step
            elif step_code in proc.masked.keys():
                dataclass = 3
                filter_['datatypes'] = step
                del filter_['pipelines']
            else:
                if self.bucket.params[0] is not None:
                    if step_code in self.bucket.params[0].datatypes:
                        dataclass = 0
                        filter_['datatypes'] = step
                        del filter_['pipelines']
                    else:
                        return None
                else:
                    return None
            return self.bucket(dataclass, copy=True, **filter_)
        else:
            filter_ = dict(ext=ext)
            if regex is not None:
                filter_['regex'] = regex
            if self.bucket.params[0] is not None:
                if step_code in self.bucket.params[0].datatypes:
                    dataclass = 0
                    filter_['datatypes'] = step_code
                    return self.bucket(dataclass, copy=True, **filter_)
            return None

    def __repr__(self):
        return self.summary

    @property
    def summary(self):
        return str(self._summary())

    def _summary(self):
        if self._pipeline_title is not None:
            self.interface.update()
            s = ['** List of existing steps in running package [{}]:\n'.format(self._pipeline_title)]
            if len(self.interface.executed) is 0:
                pass
            else:
                s.append("- Processed steps:")
                for i, step in sorted(self.interface.executed.items()):
                    s.append("\t{}: {}".format(i, step))
            if len(self.interface.reported) is 0:
                pass
            else:
                s.append("- Reported steps:")
                for i, step in sorted(self.interface.reported.items()):
                    s.append("\t{}: {}".format(i, step))
            if len(self.interface.masked) is 0:
                pass
            else:
                s.append("- Mask data:")
                for i, step in sorted(self.interface.masked.items()):
                    s.append("\t{}: {}".format(i, step))
            if len(self.interface.waiting_list) is 0:
                pass
            else:
                s.append("- Queue:")
                s.append("\t{}".format(', '.join(self.interface.waiting_list)))
            output = '\n'.join(s)
            return output
        else:
            return None


class PipelineBuilder(object):
    """ The class for building a pipeline plugin

    """
    def __init__(self, interface):
        self._interface = interface

    @property
    def interface(self):
        return self._interface

    @property
    def installed_pipelines(self):
        pipes = [pipe[5:] for pipe in dir(self) if 'pipe_' in pipe]
        output = dict(zip(range(len(pipes)), pipes))
        return output


if __name__ == '__main__':
    print(help(Pipeline))
    # pipe = Pipeline('../../examples', verbose=False)
    # pipe.set_package(0)
    # pipe.howto(0)
    # print(pipe.get_param())
    #
    # pipe._verbose = True
    # pipe.run(0)
    # pipe.check_progression()
