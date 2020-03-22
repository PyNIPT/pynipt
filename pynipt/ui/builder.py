# import os
# import re
# from ..utils import *
import time
from paralexe import Scheduler, Manager, FuncManager
import time

from ..core.base import config
from ..core.handler import InterfaceHandler
_refresh_rate = float(config.get('Preferences', 'daemon_refresh_rate'))
_timeout = float(config.get('Preferences', 'timeout'))


class InterfaceBuilder(InterfaceHandler):
    """ for building a interface plugin
    # TODO: Docstring update is needed, also need to find better way to operate pipeline on thread,
    # TODO: UserInterface need to be more intuitive. (e.g. need to have a method to test interface)

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
            self._n_threads = processor.scheduler_param['n_threads']
        else:
            self._n_threads = n_threads
        # Initiate scheduler
        self._schd = Scheduler(n_threads=self._n_threads)

    def reset(self):
        self.__init__(self._procobj, n_threads=self._n_threads)

    @property
    def threads(self):
        """ return the scheduler object """
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
        self.reset()
        run_order = self._update_run_order()
        # add current step code to the step list

        mode_dict = {'processing'   : 1,
                     'reporting'    : 2,
                     'masking'      : 3}

        if mode in mode_dict.keys():
            self._path = self._procobj.init_step(title=title, suffix=suffix,
                                                 idx=idx, subcode=subcode,
                                                 mode=mode)
            if self.step_code not in self._procobj._waiting_list:
                if self.step_code not in self._procobj._processed_list:
                    self._procobj._waiting_list.append(self.step_code)
                    self.logging('debug', 'added waiting list.',
                                 method='init_step-[{}]'.format(self.step_code))
                else:
                    # check if the current step had been processed properly
                    self._procobj.update()
                    base = {1: self._procobj._executed,
                            2: self._procobj._reported,
                            3: self._procobj._masked, }
                    if self.step_code in base[mode_dict[mode]].keys():
                        self.logging('debug',
                                     'has been processed, \n'
                                     'so this step will not be executed.',
                                     method='init_step-[{}]'.format(self.step_code))
                    else:
                        self._procobj._processed_list.remove(self.step_code)
                        self._procobj._waiting_list.append(self.step_code)
                        self.logging('debug',
                                     ' has been processed but its empty now. \n'
                                     'so it is added waiting list again.',
                                     method='init_step-[{}]'.format(self.step_code))

            else:
                self.logging('debug', ' is added waiting list.',
                             method='init_step-[{}]'.format(self.step_code))
        else:
            exc_msg = '"{}" is not an available mode.'.format(mode)
            self.logging('warn', exc_msg, method='init_step-[{}]'.format(self.step_code))

        # self._init_step(mode_dict[mode])
        daemon = self.get_daemon(self._init_step, run_order, mode_dict[mode])

        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_input(self, label, input_path, filter_dict=None, method=0, mask=False, idx=None, join_modifier=None):
        """this method sets the input with filename filter, data can be collected from dataset path or working path,
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
                                    1 - set multiple files as input to generate single master output
            mask(bool):             True if input is mask file
            idx(int):               index of the filtered dataset in order to pick one file as input
                                    across subjects or sessions.
            join_modifier(dict):    can be used when method=1. this option can be used to alter the way of
                                    listing the set of inputs.
                                    available keys={'prefix', 'suffix', # too add additional string on input paths
                                                    'spacer'}           # use given spacer between set of inputs
                                                                          (e.g. ',' or '\t') default is single space
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
        """This method will set output, if no input prior to this method, it will raise error.
        For the case of input methods 1 and 2, the output filename will be set as [subject]
        and [subject_session], respectively, and extension must be specified.

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

    @property
    def check_output(self):
        # TODO: need to delete after fix all default plugin
        # (this is for backward compatibility)
        return self.set_output_checker

    def set_output_checker(self, label='output', prefix=None, suffix=None, ext=None):
        """This method generates output filter to prevent execution if the output file is already exists.

        Args:
            label(str):     main output placeholder on command template
            prefix(str):    in case the executing command add prefix to the output filename
            suffix(str):    in case the executing command add suffix to the output filename
            ext(str):       in case the executing command add extenrion to the output filename
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_output_checker, run_order, label,
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

    def set_func(self, func):
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_func, run_order, func)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def run(self, mode=None):
        # submit job to scheduler
        run_order = self._update_run_order()
        # link this object to the parents class
        self._procobj._running_obj[self.step_code] = self
        # add current step code to the step list
        daemon = self.get_daemon(self._run, run_order, mode=mode)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def _run(self, run_order, mode=None):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, 'running interface command..', method='run') # wait until previous command is finished.
            # command process start from here
            self._inspect_output()
            if mode == 'python':
                self._mngs = self._call_func_manager()
            else:
                self._mngs = self._call_manager()
            for mng in self._mngs:
                try:
                    mng.schedule(self._schd, label=self.step_code)
                except:
                    self.logging('warn', 'exception is occurred during job scheduling.',
                                 method='run-[{}]'.format(self.step_code))
            self.logging('debug', 'processing scheduled.'.format(self.step_code),
                         method='run-[{}]'.format(self.step_code))
            self._schd.submit(mode='background', use_label=True)
            self._schd.join() # because foreground option cannot check the status
            # command process end here

            inspect_result = self._inspect_run()
            # update dataset bucket
            self.logging('debug', 'updating dataset bucket.', method='run-[{}]'.format(self.step_code))
            self._bucket.update()

            if inspect_result:
                self.logging('warn', 'missing output file(s).', method='run-[{}]'.format(self.step_code))
            # parse stdout and stderr
            self.logging('debug', 'collect STDOUT from workers.', method='run-[{}]'.format(self.step_code))

            for i, workers in self._schd.stdout.items():
                for j in sorted(workers.keys()):
                    if workers[j] is None:
                        self.logging('stdout', 'None\n')
                    else:
                        # self.logging('stdout', '\n  {}'.format('\n  '.join(workers[j]).encode('utf-8')))
                        self.logging('stdout', '\n  {}'.format('\n  '.join(workers[j])))
            self.logging('debug', 'collect STDERR from workers.', method='run-[{}]'.format(self.step_code))

            for i, workers in self._schd.stderr.items():
                for j in sorted(workers.keys()):
                    if workers[j] is None:
                        self.logging('stderr', 'None\n')
                    else:
                        self.logging('stderr', '\n  {}'.format(u'\n  '.join(workers[j])))
                        # self.logging('stderr', '\n  {}'.format(u'\n  '.join(workers[j]).encode('utf-8')))

            # step code update
            self.clear()
            # last_step_code = self._procobj._waiting_list[0]
            # if last_step_code != self.step_code:
            #     self.logging('warn', '** FATAL ERROR ** step code mismatch.',
            #                  method='run-[{}]'.format(self.step_code))
            # else:
            #     # del self._procobj._running_obj[self.step_code]
            #     self._procobj._processed_list.append(self._procobj._waiting_list.pop(0))
            # self.logging('debug', 'processed.',
            #              method='run-[{}]'.format(self.step_code))
        # update executed folder
        self._procobj.update()

    @property
    def waiting_steps(self):
        return self._procobj._waiting_list

    @property
    def processed_steps(self):
        return self._procobj._processed_list

    def remove_from_waitinglist(self):
       self.clear()

    def clear(self):
        if self.step_code is not None:
            last_step_code = self._procobj._waiting_list[0]
            if last_step_code != self.step_code:
                self.logging('warn', '** FATAL ERROR ** step code mismatch.',
                             method='run-[{}]'.format(self.step_code))
            else:
                self._procobj._processed_list.append(self._procobj._waiting_list.pop(0))
            self.logging('debug', 'processed.',
                         method='run-[{}]'.format(self.step_code))

    def get_inputs(self, label):
        input_ready = False
        while input_ready is False:
            try:
                inputs = self._input_set[label]
                if isinstance(inputs, str):   # case of input method == 1
                    inputs = self._input_set[label].split(self._input_spacer)
                    input_ready=True
                elif isinstance(inputs, list):
                    input_ready=True
                return inputs
            except:
                time.sleep(_refresh_rate)
        return

    def get_input_ref(self):
        return self._input_ref

    def run_manually(self, args, mode=None):
        loop = True
        start = time.time()
        managers = []
        if mode == 'python':
            while loop:
                time.sleep(_refresh_rate)
                if len(self._func_set.keys()) == 0:
                    if time.time() - start < _timeout:
                        pass
                    else:
                        raise Exception('[{}]-no func found'.format(self.step_code))
                else:
                    loop = False
            for j, func in sorted(self._func_set.items()):
                mng = FuncManager()
                func_kwargs = self._parse_func_kwargs(func)
                print('[{}]-arguments in given function: [{}].'.format(self.step_code, list(func_kwargs)))
                mng.set_func(func)
                for kw in func_kwargs:
                    for label, value in args.items():
                        if kw in label:
                            mng.set_arg(label=label, args=value)
                managers.append(mng)
        else:
            while loop:
                time.sleep(_refresh_rate)
                if len(self._cmd_set.keys()) == 0:
                    if time.time() - start < _timeout:
                        pass
                    else:
                        raise Exception('[{}]-no command found'.format(self.step_code))
                else:
                    loop = False

            for i, cmd in sorted(self._cmd_set.items()):
                mng = Manager()
                placeholders = self._parse_placeholder(mng, cmd)
                print('[{}]-placeholder in command template: [{}].'.format(self.step_code,
                                                                           list(placeholders)))
                mng.set_cmd(cmd)
                for ph in placeholders:
                    for label, value in args.items():
                        if ph in label:
                            mng.set_arg(label=label, args=value)
                managers.append(mng)
        for mng in managers:
            try:
                mng.schedule(self._schd, label=self.step_code)
            except:
                raise Exception('[{}]-exception is occurred during job scheduling.'.format(self.step_code))

        print('[{}]-processing scheduled.'.format(self.step_code))
        self._schd.submit(use_label=True)

        for i, workers in self._schd.stdout.items():
            for j in sorted(workers.keys()):
                if workers[j] is None:
                    print('stdout', 'None\n')
                else:
                    # print('stdout', '\n  {}'.format('\n  '.join(workers[j]).encode('utf-8')))
                    print('stdout', '\n  {}'.format('\n  '.join(workers[j])))
        for i, workers in self._schd.stderr.items():
            for j in sorted(workers.keys()):
                if workers[j] is None:
                    print('stderr', 'None\n')
                else:
                    # print('stderr', '\n  {}'.format(u'\n  '.join(workers[j]).encode('utf-8')))
                    print('stderr', '\n  {}'.format(u'\n  '.join(workers[j])))


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

