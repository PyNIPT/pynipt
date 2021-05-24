from .bucket import Bucket
from .plugin import PluginLoader
from ..config import config
from ..errors import *
from ..utils import *
from typing import Optional, Union
# from shleeh.utils import deprecated_warning
import time
from copy import copy as cp

try:
    from IPython import get_ipython
    if get_ipython() and len(get_ipython().config.keys()):
        notebook_env = True
    else:
        notebook_env = False
except ModuleNotFoundError:
    notebook_env = False

if notebook_env:
    from tqdm.notebook import tqdm as progressbar, trange
    from IPython.display import display
else:
    from pprint import pprint as display
    from tqdm import tqdm as progressbar, trange


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
        self._step_titles           = dict()
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
        # terminate all interface builders
        if self.interface is not None:
            for step_code, builder in self.interface._running_obj.copy().items():
                self.interface._running_obj[step_code]._deep_clear()
                # builder._deep_clear()
                del self.interface._running_obj[step_code]

        # detach interface
        self._interface_plugins = None

        # detach selected pipeline
        self.selected = None
        self._pipeobj = None

        # clear placeholders
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
        self._stored_id = False
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
            self._set_pkg(package_id)
        else:
            raise IndexError('Invalid package id')

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
        # TODO: need to check if this metrics work or not.
        if self._stored_id is not None:
            self._pipeobj.from_file(name, file_path)
        else:
            raise InvalidApproach('pipeline package must be running.')

    def _set_pkg(self, pkg_id: int, **kwargs):
        self._stored_id = pkg_id
        self._pipeline_title = self.installed_packages[pkg_id]
        self._interface_plugins = self._plugin.get_interfaces()(self._bucket, self._pipeline_title,
                                                                logger=self._logger,
                                                                n_threads=self._n_threads)
        self._pipeobj = self._plugin.get_pkgs(self._stored_id)
        if hasattr(self._pipeobj, self._pipeline_title):
            selected_pkg = getattr(self._pipeobj, self._pipeline_title)
            self.selected = selected_pkg(self._interface_plugins, **kwargs)

    def reset(self):
        """
        Reset pipeline instance.
        """
        self._bucket.update()
        if self._stored_id is not None:
            if isinstance(self._stored_id, bool):
                pkg_title = cp(self._pipeline_title)
                self.detach_package()
                # re-initiate scratch package
                self.set_scratch_package(pkg_title)
            else:
                pkg_id = cp(self._stored_id)
                self.detach_package()
                # re-attach the selected package
                self._set_pkg(pkg_id)
        else:
            pass

    @property
    def queued_steps(self):
        if self._interface_plugins is not None:
            return self._interface_plugins.scheduler_param['queue']
        else:
            return None

    @property
    def finished_steps(self):
        if self._interface_plugins is not None:
            return self._interface_plugins.scheduler_param['done']
        else:
            return None

    def is_failed(self, step_code: str, idx: Optional[Union[int, None]] = None) -> bool:
        """ metrics to check the selected step is failed on processing
        Args:
            step_code: step code
            idx: index for substep (default=0)
        Returns:
            True if the step failed else False
        """
        if step_code in self.schedulers.keys():
            failed_workers = self.schedulers[step_code]._failed_workers

            if len(failed_workers):
                num_fw = 0
                if idx is None:
                    for fw in failed_workers.values():
                        num_fw += len(fw)
                else:
                    num_fw += len(failed_workers[idx])
                if num_fw:
                    return True
        return False

    def check_progression(self, step_code: Union[str, None] = None):
        """Method that can realtime progression of pipeline execution."""
        if self._interface_plugins is not None:
            if step_code is None:
                # display pipeline level progress bar
                queued_jobs = len(self.queued_steps)
                finished_jobs = len(self.finished_steps)
                desc = self.installed_packages[self._stored_id] if self._stored_id is not None \
                    else self._pipeline_title

                self._progressbar = progressbar(total=queued_jobs + finished_jobs,
                                                desc=desc,
                                                initial=finished_jobs)

                def workon(n_queued, n_finished):
                    while n_finished < n_queued + n_finished:
                        delta = n_queued - len(self.queued_steps)
                        if len(self.queued_steps):
                            running_step = self.queued_steps[0]
                            if self.is_failed(running_step):
                                # alarm if the running step is failed
                                self._progressbar.sp(bar_style='danger')
                                self._progressbar.write('Pipeline has been stopped.')
                                self._stop(running_step)
                                break
                        if delta > 0:
                            n_queued -= delta
                            n_finished += delta
                            self._progressbar.update(delta)
                        time.sleep(0.2)
                    self._progressbar.close()

                import threading
                thread = threading.Thread(target=workon, args=(queued_jobs, finished_jobs))
                thread.daemon = True
                thread.start()
            else:
                # display step level progress bar
                schd = self.schedulers[step_code]

                def workon():
                    if schd._num_steps == 0:
                        if step_code not in self.interface.waiting_list:
                            print(f'Not queued: [{step_code}].')
                            return
                        else:
                            # wait until its ready
                            while schd._num_steps == 0:
                                time.sleep(0.2)
                    sup_bar = trange(schd._num_steps, desc=f'[{step_code}]')
                    for step in sup_bar:
                        n_fin_workers = len(schd._succeeded_workers[step]) \
                            if step in schd._succeeded_workers.keys() else 0
                        total_workers = len(schd._queues[step])
                        sub_bar = progressbar(total=total_workers,
                                              desc=f'substep::{step}',
                                              initial=n_fin_workers)
                        if self.is_failed(step_code, idx=step):
                            sub_bar.sp(bar_style='danger')
                            break
                        while n_fin_workers < total_workers:
                            cur_fin_workers = len(schd._succeeded_workers[step])
                            delta = cur_fin_workers - n_fin_workers
                            if delta > 0:
                                n_fin_workers += delta
                                sub_bar.update(delta)
                            time.sleep(0.2)
                        if self.is_failed(step_code, idx=step):
                            # change bar color to red if any failed workers were found
                            sub_bar.sp(bar_style='danger')
                            sup_bar.sp(bar_style='danger')
                            sub_bar.write(f'Step [{step_code}] has been stopped.')
                            self._stop(step_code)
                        sub_bar.close()

                import threading
                thread = threading.Thread(target=workon)
                thread.daemon = True
                thread.start()

    def _stop(self, step_code):
        """ Stop thread for scheduler """
        try:
            self.interface.logging('debug', 'Pipeline is stopped.')
            self.schedulers[step_code]._background_binder._tstate_lock.release()
        except AttributeError:
            self.interface.logging('debug', 'Pipeline is stopped.')

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

    def _parse_step_titles(self):
        itf = self.interface
        resources = [itf._existing_step_dir, itf._existing_report_dir,
                     itf._existing_mask_dir, itf._existing_report_dir]

        for r in resources:
            for step_code, title in r.items():
                if step_code not in self._step_titles.keys():
                    self._step_titles[step_code] = title

    def review(self, step_code):
        """ Review the executed step code, only the step executed or running in current python session
        can be reviewed. If the step failed, all stdout and stderr from command execution will be printed out.
        Args:
            step_code: the step code you want to review
        """
        self._parse_step_titles()
        step_title = self._step_titles[step_code]
        print(f'Review step [{step_code}]: {step_title}')
        message = 'The step is not executed yet.'
        if step_code in self.managers.keys():
            if self.managers[step_code] is not None:
                num_substeps = len(self.managers[step_code])
                print(f'Number of sub-steps: {num_substeps}')
            else:
                print(message)
        else:
            print(message)
        schd = self.schedulers[step_code]
        if schd.is_alive():
            status = 'Running'
            print(f'Status: {status}')
        else:
            if self.is_failed(step_code):
                status = 'Failed'
                print(f'Status: {status}')
                failed_sub_steps = []
                if len([schd._failed_steps]):
                    failed_sub_steps.extend(schd._failed_steps)
                if len([schd._incomplete_steps]):
                    failed_sub_steps.extend(schd._incomplete_steps)
                failed_sub_steps = list(set(failed_sub_steps))

                for sub_step in failed_sub_steps:
                    # print out all error messages for each worker
                    self.managers[step_code][sub_step].audit()
            else:
                status = 'Success'
                print(f'Status: {status}')

    def remove(self, step_code, mode):
        """ Remove specified step from the file system
        # TODO: considering to remove the mode option, but keeping inconvenience would be better for removing something.
        Args:
            step_code:  the step code you want to remove from file system
            mode:       the data class mode that the step you specified is locating.
                        ['processing', 'reporting', 'masking']
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
        self.bucket.reset()

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

    def get_builder(self, n_threads=None):
        """ get interface builder class that linked with current pipeline session """
        if self.interface is not None:
            from .interface import InterfaceBuilder
            if n_threads is not None:
                return InterfaceBuilder(self.interface, n_threads=n_threads)
            else:
                return InterfaceBuilder(self.interface)
        else:
            return None

    def get_dset(self, step_code: str,
                 ext: str = 'nii.gz',
                 regex: Optional[str] = None) -> (Bucket, None):
        """ the metrics to access dataset in specified step
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
            self._parse_step_titles()    # update all step titles
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
                running_step = self.queued_steps[0]
                if self.schedulers[running_step].is_alive():
                    s.append("- Running:")
                    s.append(f"\t{running_step}: {self._step_titles[running_step]}")
                else:
                    if self.is_failed(running_step):
                        # if running step is failed, stop the running step and report the issue,
                        s.append("- Issued:")
                        # below stop command does not stop running thread immediately, need wait until the error
                        # message shows up, or reset your notebook.
                        self._stop(running_step)
                    else:
                        s.append("- Pending:")
                    s.append(f"\t{running_step}: {self._step_titles[running_step]}")
                if len(self.queued_steps) > 1:
                    s.append("- Queue:")
                    s.append("\t{}".format(', '.join(self.queued_steps[1:])))
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
