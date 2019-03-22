import os
import re
import time
from paralexe import Scheduler

from ..core.base import config
from ..core.handler import InterfaceHandler
from ..utils import *
_refresh_rate = float(config.get('Preferences', 'daemon_refresh_rate'))


class InterfaceBuilder(InterfaceHandler):
    """ The class for building a interface plugin
    # TODO: update docstring, find better way than using daemon

    Methods:
        init_step:
        is_initiated:
        set_input:
        set_output:
        set_temporary:
        set_var:
        set_cmd:

    Attributes:
        path:
    """
    def __init__(self, processor, n_threads=None):
        super(InterfaceBuilder, self).__init__()
        self._parse_info_from_processor(processor)
        self._init_attr_for_inspection()
        self._init_attr_for_execution()
        if n_threads is None:
            # Initiate scheduler
            self._schd = Scheduler(n_threads=processor.scheduler_param['n_threads'])
        else:
            self._schd = Scheduler(n_threads=n_threads)

    @property
    def threads(self):
        return self._schd

    def init_step(self, title, suffix=None, idx=None, subcode=None, mode='processing'):
        """initiate step directory, this includes generating step code and creating the directory

        Args:
            title(str):     the title for the step directory
            suffix(str):    suffix for the title
            idx:            index of step
            subcode:        sub-step code to identify the sub-step process
            mode (str):     'processing'- create step directory in working path
                            'reporting' - create step directory in result path
                            'masking'   - create step directory in mask path

        Raises:
            Exception:      if wrong mode are inputted

        """
        run_order = self._update_run_order()
        # add current step code to the step list

        mode_dict = {'processing': 1,
                     'reporting': 2,
                     'masking': 3}

        if mode in mode_dict.keys():
            self._path = self._procobj.init_step(title=title, suffix=suffix,
                                                 idx=idx, subcode=subcode,
                                                 mode=mode)
            if self.step_code not in self._procobj._waiting_list:
                if self.step_code not in self._procobj._processed_list:
                    self._procobj._waiting_list.append(self.step_code)
                    self.logging('debug', '[{}] is added waiting list.'.format(self.step_code),
                                 method='init_step')
                else:
                    # check if the current step had been processed properly
                    self._procobj.update()
                    base = {1: self._procobj._executed,
                            2: self._procobj._reported,
                            3: self._procobj._masked, }
                    if self.step_code in base[mode_dict[mode]].keys():
                        self.logging('debug',
                                     '[{}] has been processed, \n'
                                     'so this step will not be executed.'.format(self.step_code),
                                     method='init_step')
                    else:
                        self._procobj._processed_list.remove(self.step_code)
                        self._procobj._waiting_list.append(self.step_code)
                        self.logging('debug',
                                     '[{}] has been processed but its empty now. \n'
                                     'so it is added waiting list again.'.format(self.step_code),
                                     method='init_step')

            else:
                self.logging('debug', '[{}] is added waiting list.'.format(self.step_code),
                             method='init_step')
        else:
            exc_msg = '[{}] is not an available mode.'.format(mode)
            self.logging('warn', exc_msg, method='init_step')

        # self._init_step(mode_dict[mode])
        daemon = self.get_daemon(self._init_step, run_order, mode_dict[mode])

        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_input(self, label, input_path, filter_dict=None, method=0, mask=False, idx=None, join_modifier=None):
        """set the input with filename filter, data can be collected from dataset path or working path,
        as well as masking path if mask is set to True.
        At least one input path need to be set for building interface.

        If there is multiple input, the total number of jobs will be established from first set of input
        and all other inputs must have the same number with first input. (inspection function will check it.)

        The method keyword argument is provided the option to apply group statistic,
        in the regular cases, each command take one master input to create the processed one master output
        which in the case of method=0. If the method=1 is given, it takes all filtered data using provided filter_dict
        and treat it as single input, which is useful for running statistics.

        Args:
            label(str):             label for space holder on command template
            input_path(str):        absolute path, step directory, datatype or step code
            filter_dict(dict):      filter set for parsing input data.
                                    available keys={'contains', 'ignore', 'ext', 'regex', # for filename
                                                    'subjects', 'sessions}                # for select specific group
            method (int):           0 - run command for the single master file to generate single master output
                                    1 - set multiple files as single input to generate single master output
            mask(bool):             True if input is mask file
            idx(int):               file order index to pick only one file as input across subjects or sessions
            join_modifier(dict):    can be used when method=1. to alter the way listing the set of inputs
                                    available keys={'prefix', 'suffix', # too add additional string on input paths
                                                    'spacer'}           # use given spacer instead single space (e.g. ',' or '\t')
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_input, run_order, label, input_path,
                                 filter_dict=filter_dict, method=method, mask=mask, idx=idx,
                                 join_modifier=join_modifier)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_static_input(self, label, input_path, filter_dict=None, idx=0, mask=False):
        run_order = self._update_run_order()
        daemon = self.get_daemon(self._set_static_input, run_order, label, input_path,
                                 filter_dict=filter_dict, idx=idx, mask=mask)
        self._daemons[run_order] = daemon

    def set_output(self, label, prefix=None, suffix=None, modifier=None, ext=None):
        """method to set output, if no input prior to this method, this method will raise error.
        For input method cases 1 and 2, the output filename will be set as [subject]
        and [subject_session] respectively, this cases, extension need to be specified.

        Args:
            label(str):             output place-holder for command template,
                                    'output' will help to prevent repetition of finished step
            prefix(str):
            suffix(str):
            modifier(dict or str):  key(find):value(replace) or file(folder)name
                                    in case the input method was set to 1,
                                    user can specify file or folder name of output
            ext(str or False):      extension if it need to be changed. If False, extension will be removed.
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_output, run_order, label,
                                 modifier=modifier, ext=ext, prefix=prefix, suffix=suffix)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def check_output(self, label='output', prefix=None, suffix=None, ext=None):
        """generate output filter to prevent execution if the output file is already exists.

        Args:
            label(str):     main output placeholder on command template
            prefix(str):    in case the executing command add prefix to the output filename
            suffix(str):    in case the executing command add suffix to the output filename
            ext(str):       in case the executing command add extenrion to the output filename
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._check_output, run_order, label,
                                 prefix=prefix, suffix=suffix, ext=ext)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_temporary(self, label, path_only=False):
        """method to set temporary output step. the structure of temporary folder

        Args:
            label(str):     temporary output place-holder for command template.
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_temporary, run_order, label, path_only)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_var(self, label, value, quote=False):
        """ If no input prior to this method, raise error

        Args:
            label(str):                 place-holder of variable for command template.
            value(str, int, or list):   value to set as variable on command template
            quote(bool):                True if the value need to be encapsulated by the quote on command line
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_var, run_order, label, value,
                                 quote=quote)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_cmd(self, command):
        """If no input prior to this method, raise error

        Args:
            command(str):   command template, use decorator '*[label]' to place arguments.
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_cmd, run_order, command)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def run(self):
        # submit job to scheduler
        run_order = self._update_run_order()
        # link this object to the parents class
        self._procobj._stepobjs[self.step_code] = self
        # add current step code to the step list
        daemon = self.get_daemon(self._run, run_order)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def _run(self, run_order):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, 'running interface command..', method='run') # wait until previous command is finished.
            # command process start from here
            self._inspect_output()
            self._mngs = self._call_manager()
            for mng in self._mngs:
                try:
                    mng.schedule(self._schd, label=self.step_code)
                except:
                    self.logging('warn', 'exception is occurred during job scheduling.'
                                         'please double check if the arguments are correctly matched with command.',
                                 method='run-[{}]'.format(self.step_code))
            self.logging('debug', 'job scheduled by the manager.'.format(self.step_code),
                         method='run-[{}]'.format(self.step_code))
            self._schd.submit(mode='background', use_label=True)
            self._schd.join() # because foreground option cannot check the status
            # command process end here

            # update dataset bucket
            self.logging('debug', 'update dataset bucket.', method='run-[{}]'.format(self.step_code))
            self._bucket.update()

            # parse stdout and stderr
            self.logging('debug', 'collect STDOUT from workers.', method='run-[{}]'.format(self.step_code))
            for i, workers in self._schd.stdout.items():
                for j in sorted(workers.keys()):
                    if workers[j] is None:
                        self.logging('stdout', 'None\n')
                    else:
                        self.logging('stdout', '\n{}'.format('\n'.join(workers[j])))
            self.logging('debug', 'collect STDERR from workers.', method='run-[{}]'.format(self.step_code))
            for i, workers in self._schd.stderr.items():
                for j in sorted(workers.keys()):
                    if workers[j] is None:
                        self.logging('stderr', 'None\n')
                    else:
                        self.logging('stderr', '\n{}'.format('\n'.join(workers[j])))

            # step code update
            last_step_code = self._procobj._waiting_list[0]
            if last_step_code != self.step_code:
                self.logging('warn', '[{}]-something got wrong, step code missmatch, '
                                     'which can cause serious problem'.format(self.step_code),
                             method='run')
            else:
                self._procobj._processed_list.append(self._procobj._waiting_list.pop(0))
            self.logging('debug', '[{}]-removed from the waiting list'.format(self.step_code),
                         method='run')


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

