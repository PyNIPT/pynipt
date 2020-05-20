import time
from typing import Any, Optional, Union, Callable
from .processor import Processor
from paralexe import Manager, FuncManager, Scheduler
from ..config import config
from ..utils import *
from ..errors import *


class InterfaceBase:
    """ Base class of InterfaceBuilder.

    Attributes and some function for getting inheritance from the other class
    is defining here in yhr base class
    """
    def __init__(self):
        # private
        self._path = None
        self._type = None
        pass

    def _parse_info_from_processor(self, processor):
        self._procobj = processor
        self._msi = processor.msi
        self._bucket = self._procobj.bucket
        self._label = processor.label
        self._multi_session = self._bucket.is_multi_session()

    def _init_attr_for_inspection(self):
        # attributes for the inspection
        self._main_input = None
        self._input_method = None
        self._input_ref = dict()
        self._input_set = dict()
        self._output_set = dict()
        self._output_filter = list()
        self._var_set = dict()
        self._temporary_set = dict()
        self._cmd_set = dict()
        self._func_set = dict()
        self._input_spacer = None

    def _init_attr_for_execution(self):
        # attributes for controlling method execution
        self._step_processed = False  # True if current step listed in procobj._processed_list
        self._order_counter = 0
        self._processed_run_order = []
        self._daemons = dict()
        self._mngs = None
        self._errterm = None
        self._refresh_rate = config['Preferences'].getfloat('daemon_refresh_rate')
        self._timeout = config['Preferences'].getint('timeout')

    @property
    def get_daemon(self):
        return self._procobj.get_daemon

    @property
    def mngs(self):
        return self._mngs

    @property
    def msi(self):
        return self._msi

    def logging(self, level: str, message: str, method: str = None):
        """ The helper method to log message. If the logger is not initiated,
        only log the message if logger is initiated.

        Args:
            level (str): 'debug', 'stdout', or 'stderr'
            message (str): The message for logging.
            method

        Raises:
            ErrorInThread: if level is not provided.
        """
        classname = 'InterfaceBuilder'
        if method is not None:
            message = '{}:{}-{}'.format(classname, method, message)
        else:
            message = '{}\n  {}'.format(classname, message)
        self._procobj.logging(level, message)
        if level == 'warn':
            raise ErrorInThread(message)

    @property
    def path(self):
        return self._path

    @property
    def step_code(self):
        if self._path is None:
            return None
        else:
            pattern = self._procobj.step_code_pattern
            step_code = pattern.sub(r'\1', self.msi.path.basename(self._path))
            return step_code

    def is_initiated(self):
        if self._path is not None:
            return True
        else:
            return False

    def _update_run_order(self):
        run_order = int(self._order_counter)
        self._order_counter += 1
        return run_order

    def _wait_my_turn(self, run_order, message=None, method=None):
        previous_run_order = run_order - 1
        loop = True
        while loop is True:
            if self.is_initiated():
                if previous_run_order in self._processed_run_order:
                    if not self._daemons[previous_run_order].is_alive():
                        loop = False
            time.sleep(self._refresh_rate)
        if message is not None:
            self.logging('debug', '[{}]-#{}-{}'.format(self.step_code,
                                                       str(run_order).zfill(3),
                                                       message), method=method)

    def _report_status(self, run_order):
        self._processed_run_order.append(run_order)


class InterfaceHandler(InterfaceBase):
    _errterm: str or list

    def __init__(self):
        super(InterfaceHandler, self).__init__()
        self._schd = None

    def _init_step(self, run_order, mode_idx):
        """ hidden method for init_step to run on the thread """
        # check if the init step is not first command user was executed
        if run_order != 0:
            self.logging('warn', 'init_step must be perform first for building command interface.',
                         method='init_step')

        if len(self._procobj.waiting_list) is 0:
            # the step_code exists in the processed_list, so no need to wait
            self._step_processed = True
        else:
            loop = True
            while loop is True:
                if self.step_code == self._procobj.waiting_list[0]:
                    loop = False
                time.sleep(self._refresh_rate)

        self._procobj.bucket.update()
        if mode_idx is not 2:
            try:
                self._procobj.update_attributes(mode_idx)
            except IndexError:
                self._procobj.update_attributes(1)
            except:
                raise UnexpectedError

        self.logging('debug', '[{}]-step initiated.'.format(self.step_code),
                     method='init_step')
        self._report_status(run_order)

    def _set_input(self, run_order, label, input_path, filter_dict,
                   method, idx, mask, join_modifier, relpath):
        """ hidden layer to run on daemon """
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order,
                               'input_path [{}] is assigned to the label [{}].'.format(label, input_path),
                               method='set_input')

            method_name = 'set_input'
            num_input_set = len(self._input_set.keys())

            if num_input_set > 0:
                if self._input_method != method:
                    exc_msg = 'invalid input method for the assigned input(s).'
                    self.logging('warn', exc_msg, method=method_name)
                else:
                    if label in self._input_set.keys():
                        exc_msg = 'duplicated label.'
                        self.logging('warn', exc_msg, method=method_name)
            else:
                self._input_method = method
                self._main_input = label

            input_path = self._procobj.inspect_input(input_path, mask=mask)
            if filter_dict is None:
                filter_dict = dict()
            else:
                exc_msg = 'invalid filter_dict.'
                if isinstance(filter_dict, dict):
                    for key in filter_dict.keys():
                        if key not in self._bucket.fname_keys:
                            self.logging('warn', exc_msg, method=method_name)
                else:
                    self.logging('warn', exc_msg, method=method_name)

            if self._input_method == 0:
                if idx is None:
                    # point to point matching between input and output
                    if input_path in self._bucket.params[0].datatypes:
                        dset = self._bucket(0, datatypes=input_path, **filter_dict)
                    else:
                        if mask is True:
                            dset = self._bucket(3, datatypes=input_path, **filter_dict)
                        else:
                            dset = self._bucket(1, pipelines=self._label, steps=input_path, **filter_dict)
                    if len(dset) > 0:
                        if num_input_set == 0:
                            if self._multi_session is True:
                                self._input_ref = {i: (finfo.Subject, finfo.Session) for i, finfo in dset}
                            else:
                                self._input_ref = {i: (finfo.Subject, None) for i, finfo in dset}
                        if relpath:
                            self._input_set[label] = [os.path.relpath(finfo.Abspath) for i, finfo in dset]
                        else:
                            self._input_set[label] = [finfo.Abspath for i, finfo in dset]
                else:
                    self._input_set[label] = list()
                    if isinstance(idx, int):
                        if input_path in self._bucket.params[0].datatypes:
                            for sub in self._bucket.params[0].subjects:
                                if self._multi_session:
                                    for ses in self._bucket.params[0].sessions:
                                        dset = self._bucket(0, datatypes=input_path,
                                                            subjects=sub, sessions=ses,
                                                            **filter_dict)
                                        if len(dset) > 0:
                                            self._input_ref[len(self._input_set[label])] = (
                                                dset[idx].Subject, dset[idx].Session)
                                            if relpath:
                                                self._input_set[label].append(os.path.relpath(dset[idx].Abspath))
                                            else:
                                                self._input_set[label].append(dset[idx].Abspath)
                                else:
                                    dset = self._bucket(0, datatypes=input_path,
                                                        subjects=sub, **filter_dict)
                                    if len(dset) > 0:
                                        self._input_ref[len(self._input_set[label])] = (
                                            dset[idx].Subject, None)
                                        if relpath:
                                            self._input_set[label].append(os.path.relpath(dset[idx].Abspath))
                                        else:
                                            self._input_set[label].append(dset[idx].Abspath)
                        else:
                            if mask is True:
                                for sub in self._bucket.params[3].subjects:
                                    if self._multi_session:
                                        for ses in self._bucket.params[3].sessions:
                                            dset = self._bucket(3, datatypes=input_path,
                                                                subjects=sub, sessions=ses,
                                                                **filter_dict)
                                            if len(dset) > 0:
                                                self._input_ref[len(self._input_set[label])] = (
                                                    dset[idx].Subject, dset[idx].Session)
                                                if relpath:
                                                    self._input_set[label].append(os.path.relpath(dset[idx].Abspath))
                                                else:
                                                    self._input_set[label].append(dset[idx].Abspath)
                                    else:
                                        dset = self._bucket(3, datatypes=input_path,
                                                            subjects=sub, **filter_dict)
                                        if len(dset) > 0:
                                            self._input_ref[len(self._input_set[label])] = (
                                                dset[idx].Subject, None)
                                            if relpath:
                                                self._input_set[label].append(os.path.relpath(dset[idx].Abspath))
                                            else:
                                                self._input_set[label].append(dset[idx].Abspath)
                            else:
                                for sub in self._bucket.params[1].subjects:
                                    if self._multi_session:
                                        for ses in self._bucket.params[1].sessions:
                                            dset = self._bucket(1, pipelines=self._label,
                                                                steps=input_path,
                                                                subjects=sub, sessions=ses,
                                                                **filter_dict)
                                            if len(dset) > 0:
                                                self._input_ref[len(self._input_set[label])] = (
                                                    dset[idx].Subject, dset[idx].Session)
                                                if relpath:
                                                    self._input_set[label].append(os.path.relpath(dset[idx].Abspath))
                                                else:
                                                    self._input_set[label].append(dset[idx].Abspath)
                                    else:
                                        dset = self._bucket(1, pipelines=self._label,
                                                            steps=input_path,
                                                            subjects=sub,
                                                            **filter_dict)
                                        if len(dset) > 0:
                                            self._input_ref[len(self._input_set[label])] = (
                                                dset[idx].Subject, None)
                                            if relpath:
                                                self._input_set[label].append(os.path.relpath(dset[idx].Abspath))
                                            else:
                                                self._input_set[label].append(dset[idx].Abspath)
                    else:
                        self.logging('warn', 'invalid index for input data',
                                     method=method_name)

            elif self._input_method == 1:
                # peer to point
                if input_path in self._bucket.params[0].datatypes:
                    dset = self._bucket(0, datatypes=input_path, **filter_dict)
                else:
                    if mask is True:
                        dset = self._bucket(3, datatypes=input_path, **filter_dict)
                    else:
                        dset = self._bucket(1, pipelines=self._label, steps=input_path,
                                            **filter_dict)
                if num_input_set == 0:
                    self._input_ref = dict()
                if self._multi_session:
                    self._input_ref[label] = [(finfo.Subject, finfo.Session) for i, finfo in dset]
                else:
                    self._input_ref[label] = [(finfo.Subject, None) for i, finfo in dset]
                if relpath:
                    list_of_inputs = [os.path.relpath(finfo.Abspath) for i, finfo in dset]
                else:
                    list_of_inputs = [finfo.Abspath for i, finfo in dset]
                spacer = ' '
                if join_modifier is not None:
                    if isinstance(join_modifier, dict):
                        if 'suffix' in join_modifier.keys():
                            list_of_inputs = ["{}{}".format(f, join_modifier['suffix']) for f in list_of_inputs]
                        if 'prefix' in join_modifier.keys():
                            list_of_inputs = ["{}{}".format(f, join_modifier['prefix']) for f in list_of_inputs]
                        if 'spacer' in join_modifier.keys():
                            spacer = join_modifier['spacer']
                    elif join_modifier is False:
                        spacer = None
                    else:
                        self.logging('warn', 'inappropriate join_modifier used',
                                     method=method_name)
                if spacer is not None:
                    self._input_set[label] = spacer.join(list_of_inputs)
                else:
                    self._input_set[label] = [list_of_inputs]
                self._input_spacer = spacer
            else:
                exc_msg = 'method selection is out of range.'
                self.logging('warn', exc_msg, method=method_name)
            self._report_status(run_order)

    def _set_static_input(self, run_order, label, input_path, filter_dict, idx, mask, relpath):
        """ hidden layer to run on daemon """
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}-{}'.format(label, input_path),
                               method='set_static_input')

            method_name = 'set_static_input'

            if self._main_input is None:
                exc_msg = 'no prior input set, run "set_input" method first.'
                self.logging('warn', exc_msg, method=method_name)
            else:
                self._inspect_label(label, method_name)

            if filter_dict is None:
                filter_dict = dict()
            else:
                exc_msg = 'insufficient filter_dict.'
                if isinstance(filter_dict, dict):
                    for key in filter_dict.keys():
                        if key not in self._bucket.fname_keys:
                            self.logging('warn', exc_msg, method=method_name)
                else:
                    self.logging('warn', exc_msg, method=method_name)

            if self._input_method is not 0:
                exc_msg = 'static_input is only allowed only for group_method=False'
                self.logging('warn', exc_msg, method=method_name)
            else:
                self._input_set[label] = list()
                for i, f_abspath in enumerate(self._input_set[self._main_input]):
                    subj, sess = self._input_ref[i]

                    if input_path in self._bucket.params[0].datatypes:
                        if sess is None:
                            dset = self._bucket(0, datatypes=input_path,
                                                subjects=subj,
                                                **filter_dict)
                        else:
                            dset = self._bucket(0, datatypes=input_path,
                                                subjects=subj, sessions=sess,
                                                **filter_dict)
                    else:
                        if mask is True:
                            if sess is None:
                                dset = self._bucket(3, datatypes=input_path,
                                                    subjects=subj,
                                                    **filter_dict)
                            else:
                                dset = self._bucket(3, datatypes=input_path,
                                                    subjects=subj, sessions=sess,
                                                    **filter_dict)
                        else:
                            if sess is None:
                                dset = self._bucket(1, pipelines=self._label, steps=input_path,
                                                    subjects=subj,
                                                    **filter_dict)
                            else:
                                dset = self._bucket(1, pipelines=self._label, steps=input_path,
                                                    subjects=subj, sessions=sess,
                                                    **filter_dict)
                    # TODO: below code will pick the indexed file. it could be list of files in case of python?
                    if relpath:
                        self._input_set[label].append(os.path.relpath(dset[idx].Abspath))
                    else:
                        self._input_set[label].append(dset[idx].Abspath)
            self._report_status(run_order)

    def set_errterm(self, error_term: str or list):
        """Set terms to indicate error condition
        Args:
            error_term:     keyword to indicate error on running subprocess, only required for shell command interface
        """
        self._errterm = error_term

    def _set_output(self, run_order, label, modifier, prefix, suffix, ext):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}'.format(label), method='set_output')

            method_name = 'set_output'
            input_name = self._main_input
            if self._main_input is None:
                exc_msg = 'no input set found, please run "set_input" method before set output.'
                self.logging('warn', exc_msg, method=method_name)
            else:
                self._inspect_label(label, method_name)

            def check_modifier(fname):
                if modifier is not None:
                    if isinstance(modifier, dict):
                        for f, rep in modifier.items():
                            fname = change_fname(fname, f, rep)
                    elif isinstance(modifier, str):
                        if self._input_method is not 1:
                            self.logging('warn',
                                         'single output name assignment is only available for input method=1',
                                         method=method_name)
                        else:
                            fname = modifier
                    else:
                        self.logging('warn', 'wrong modifier.', method=method_name)
                    fn, fext = split_ext(fname)
                    if fext is None:
                        fext = 'dir'
                    if prefix is not None:
                        fn = '{}{}'.format(prefix, fn)
                    if suffix is not None:
                        fn = '{}{}'.format(fn, suffix)
                    fname = '.'.join([fn, fext])

                    if ext is not None:
                        if isinstance(ext, str):
                            fname = change_ext(fname, ext)
                        elif not ext:
                            fname = remove_ext(fname)
                        else:
                            self.logging('warn', '[{}]-wrong extension.'.format(self.step_code), method=method_name)
                else:
                    if self._input_method == 1:
                        fname = '{}_output'.format(self.step_code)
                        if prefix is not None:
                            fname = '{}{}'.format(prefix, fname)
                        if suffix is not None:
                            fname = '{}{}'.format(fname, suffix)
                        if ext is not None:
                            fname = '{}.{}'.format(fname, ext)
                    else:
                        fn, fext = split_ext(fname)
                        if prefix is not None:
                            fn = '{}{}'.format(prefix, fn)
                        if suffix is not None:
                            fn = '{}{}'.format(fn, suffix)
                        fname = '.'.join([fn, fext])
                        if ext is not None:
                            if isinstance(ext, str):
                                fname = change_ext(fname, ext)
                            elif not ext:
                                fname = remove_ext(fname)
                            else:
                                self.logging('warn',
                                             '[{}]-invalid extension.'.format(self.step_code),
                                             method=method_name)
                return fname

            # all possible input types, method 0 and method 1
            self._output_set[label] = []
            if self._input_method == 0:
                for i, f_abspath in enumerate(self._input_set[input_name]):
                    subj, sess = self._input_ref[i]

                    if sess is None:
                        output_path = self.msi.path.join(self._path, subj)
                    else:
                        output_path = self.msi.path.join(self._path, subj, sess)

                    filename = check_modifier(os.path.basename(f_abspath))
                    self._output_set[label].append((output_path, filename))

            elif self._input_method == 1:
                filename = check_modifier(modifier)
                self._output_set[label].append((self._path, filename))

            else:
                exc_msg = '[{}]-unexpected error, might be caused by invalid input_method.'.format(self.step_code)
                self.logging('warn', exc_msg, method=method_name)

            self._report_status(run_order)

    def _set_output_checker(self, run_order, label, prefix, suffix, ext):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}'.format(label), method='set_output_checker')
            method_name = 'check_output'

            for lb, v in self._output_set.items():
                if lb == label:
                    if self._input_method == 0:
                        for p, fn in v:
                            fn_woext, old_ext = split_ext(fn)
                            if prefix is not None:
                                fn_woext = '{}{}'.format(prefix, fn_woext)
                            if suffix is not None:
                                fn_woext = '{}{}'.format(fn_woext, suffix)
                            if ext is not None:
                                fn = '{}.{}'.format(fn_woext, ext)
                            else:
                                fn = '{}.{}'.format(fn_woext, old_ext)
                            self._output_filter.append((p, fn))
                    elif self._input_method == 1:  # input_method=1 has only one master output
                        if isinstance(v[0], tuple) and len(v[0]) == 2:
                            p, fn = v[0]
                            fn_woext, old_ext = split_ext(fn)
                            if prefix is not None:
                                fn_woext = '{}{}'.format(prefix, fn_woext)
                            if suffix is not None:
                                fn_woext = '{}{}'.format(fn_woext, suffix)
                            if ext is not None:
                                fn = '{}.{}'.format(fn_woext, ext)
                            else:
                                fn = '{}.{}'.format(fn_woext, old_ext)
                            self._output_filter.append((p, fn))
                        else:
                            exc_msg = '[{}]-unexpected error, ' \
                                      'this error can be caused by incorrect input_method.'.format(self.step_code)
                            self.logging('warn', exc_msg, method=method_name)

            if len(self._output_filter) == 0:
                self.logging('debug', 'Exception detected: [Please check below information]\n'
                                      'input_method={},\n'
                                      'detected_output={}\n'.format(self._input_method, self._output_set.items()))
                self.logging('warn', '[{}]-No output_filter, this exception can be caused by '
                                     'insufficient regular expression pattern.'.format(self.step_code),
                             method='set_output_checker')
            self._report_status(run_order)

    def _set_temporary(self, run_order, label, path_only, relpath):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}'.format(label), method='set_temporary')

            method_name = 'set_temporary'
            input_name = self._main_input

            if self._main_input is None:
                exc_msg = '[{}]-cannot find input set, run set_input method first.'.format(self.step_code)
                self.logging('warn', exc_msg, method=method_name)
            else:
                if self._input_method != 0:
                    if path_only is False:
                        exc_msg = '[{}]-cannot use temporary step when group_input is True.'.format(self.step_code)
                        self.logging('warn', exc_msg, method=method_name)
                self._inspect_label(label, method_name)

            step_path = self.msi.path.basename(self.path)
            if relpath:
                temp_path = os.path.relpath(self._procobj.temp_path)
            else:
                temp_path = self._procobj.temp_path
            if path_only is True:
                self._temporary_set[label] = self.msi.path.join(temp_path, step_path)
            else:
                self._procobj.update_attributes(1)
                self._temporary_set[label] = []
                for i, f_abspath in enumerate(self._input_set[input_name]):
                    subj, sess = self._input_ref[i]
                    if sess is None:
                        output_path = self.msi.path.join(temp_path, step_path, subj)
                    else:
                        output_path = self.msi.path.join(temp_path, step_path, subj, sess)
                    filename = os.path.basename(f_abspath)
                    self._temporary_set[label].append((output_path, filename))

            self._report_status(run_order)

    def _set_var(self, run_order, label, value, quote):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}-{}'.format(label, value), method='set_var')
            method_name = 'set_var'
            self._inspect_label(label, method_name)
            if self._type == 'cmd':
                # convert value to string for command-line execution
                if isinstance(value, list):
                    if quote is True:
                        value = ["'{}'".format(v) for v in value]
                elif isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
                    value = str(value)
                    if quote is True:
                        value = '"{}"'.format(value)
                else:
                    exc_msg = '[{}]-incorrect variable.'.format(self.step_code)
                    self.logging('warn', exc_msg, method=method_name)
            elif self._type == 'python':
                # preserve datatype for mode=python
                pass
            else:
                raise InvalidApproach(f'Invalid step type: {self._type}')
            self._var_set[label] = value
            self._report_status(run_order)

    def _set_cmd(self, run_order, command):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            if self._type == 'cmd':
                self._wait_my_turn(run_order, '{}'.format(command), method='set_cmd')
                self._cmd_set[len(self._cmd_set.keys())] = command
                self._report_status(run_order)
            else:
                raise InvalidApproach('Use set_func instead.')

    def _set_func(self, run_order, func):
        if self._step_processed is True:
            pass
        else:
            if self._type == 'python':
                func_name = func.__code__.co_name
                n_args = func.__code__.co_argcount
                keywords = func.__code__.co_varnames[:n_args]
                func_code = '{}({})'.format(func_name, ','.join(keywords))

                self._wait_my_turn(run_order, '{}'.format(func_code), method='set_func')
                self._func_set[len(self._func_set.keys())] = func
                self._report_status(run_order)
            else:
                raise InvalidApproach('Use set_cmd instead.')

    def _inspect_label(self, label, method_name=None):

        inspect_items = [self._input_set, self._output_set, self._var_set, self._temporary_set]
        for item in inspect_items:
            if label in item.keys():
                exc_msg = '[{}]-The label "{}" is duplicated.'.format(self.step_code, label)
                self.logging('warn', exc_msg, method=method_name)

    @staticmethod
    def _parse_placeholder(manager, command):
        prefix, suffix = manager.decorator
        raw_prefix = ''.join([r'\{}'.format(c) for c in prefix])
        raw_suffix = ''.join([r'\{}'.format(c) for c in suffix])

        # The text
        p = re.compile(r"{0}[^{0}{1}]+{1}".format(raw_prefix, raw_suffix))
        return set([obj[len(prefix):-len(suffix)] for obj in p.findall(command)])

    @staticmethod
    def _parse_func_kwargs(func):
        n_args = func.__code__.co_argcount
        return [kw for kw in func.__code__.co_varnames[:n_args] if kw not in ['stdout', 'stderr']]

    def _inspect_output(self):
        """This hidden method detects output files that created before
        """
        msi = self.msi
        method = '_inspect_output'
        index_for_filter = []
        if len(self._output_filter):
            for i, (path, fname) in enumerate(self._output_filter):
                abspath = msi.path.join(path, fname)
                if msi.path.exists(abspath):
                    self.logging('debug', 'File exists: [{}]'.format(fname),
                                 method=f'{method}-[{self.step_code}]')
                    index_for_filter.append(i)
            if len(index_for_filter) > 0:
                self.logging('debug',
                             f'-Total of {len(index_for_filter)} file(s) are skipped from re-processing.'
                             'Please remove the file(s) listed above if it needs to be re-processed.',
                             method=f'{method}-[{self.step_code}]')
                arg_sets = [self._input_set, self._output_set, self._var_set, self._temporary_set]
                for arg_set in arg_sets:
                    for label, value in arg_set.items():
                        if isinstance(value, list):
                            arg_set[label] = [v for i, v in enumerate(value) if i not in index_for_filter]
            else:
                self.logging('debug', f'Passed',
                             method=f'{method}-[{self.step_code}]')
        else:
            self.logging('debug', 'No output filter found. Inspection has been skipped.',
                         method=f'{method}-[{self.step_code}]')

    def _inspect_run(self):
        """This hidden method will check if the interface was run properly by checking output
        """
        msi = self.msi
        method = '_inspect_run'
        index_for_filter = []
        if len(self._output_filter):
            for i, (path, fname) in enumerate(self._output_filter):
                abspath = msi.path.join(path, fname)
                if not msi.path.exists(abspath):
                    self.logging('debug', 'File does not created: [{}]'.format(fname),
                                 method=f'{method}-[{self.step_code}]')
                    index_for_filter.append(i)
            if len(index_for_filter) > 0:
                self.logging('debug',
                             f'-Total of {len(index_for_filter)} workers are failed.',
                             method=f'{method}-[{self.step_code}]')
                return 1
            else:
                self.logging('debug', 'Success.'.format(self.step_code),
                             method=f'{method}-[{self.step_code}]')
                return 0
        else:
            self.logging('debug', 'No output filter found. Inspection has been skipped.',
                         method=f'{method}-[{self.step_code}]')
            return 1

    def _call_manager(self):
        """This method calls the Manager instance and set the command template with its arguments on it.
        """
        managers = []
        if len(self._cmd_set.keys()) == 0:
            self.logging('warn', '[{}]-no command found'.format(self.step_code),
                         method='_call_manager')
        for j, cmd in sorted(self._cmd_set.items()):
            mng = Manager()
            placeholders = self._parse_placeholder(mng, cmd)
            self.logging('debug', '[{}]-placeholder in command template: [{}].'.format(self.step_code,
                                                                                       list(placeholders)),
                         method='_call_manager')
            mng.set_cmd(cmd)
            arg_sets = [self._input_set, self._output_set, self._var_set, self._temporary_set]
            for i, arg_set in enumerate(arg_sets):
                for ph in placeholders:
                    for label, value in arg_set.items():
                        if isinstance(value, list):
                            if len(value) == 0:
                                pass
                            else:
                                if isinstance(value[0], tuple):
                                    joined_values = []
                                    for v in value:
                                        intensive_mkdir(v[0], interface=self.msi)
                                        joined_values.append(self.msi.path.join(*v))
                                    value = joined_values
                        elif isinstance(value, str):
                            if i is 3:
                                # the case for the set_temporary has been initiated with path_only=True
                                intensive_mkdir(value, interface=self.msi)
                        if ph in label:
                            mng.set_arg(label=label, args=value)

            if self._errterm is not None:
                mng.set_errterm(self._errterm)
            managers.append(mng)
            self.logging('debug', '[{}]-managers instance receives all required information.'.format(self.step_code),
                         method='_call_manager')
        return managers

    def _call_func_manager(self):
        managers = []
        if len(self._func_set.keys()) == 0:
            self.logging('warn', '[{}]-no python function found'.format(self.step_code),
                         method='_call_func_manager')

        for j, func in sorted(self._func_set.items()):
            mng = FuncManager()
            func_kwargs = self._parse_func_kwargs(func)
            self.logging('debug', '[{}]-arguments in function: [{}].'.format(self.step_code,
                                                                             list(func_kwargs)),
                         method='_call_func_manager')
            mng.set_func(func)
            arg_sets = [self._input_set, self._output_set, self._var_set, self._temporary_set]

            for i, arg_set in enumerate(arg_sets):
                for kw in func_kwargs:
                    for label, value in arg_set.items():
                        # create folder
                        if isinstance(value, list):
                            if len(value) == 0:
                                pass
                            else:
                                if isinstance(value[0], tuple):
                                    joined_values = []
                                    for v in value:
                                        intensive_mkdir(v[0], interface=self.msi)
                                        joined_values.append(self.msi.path.join(*v))
                                    value = joined_values
                        elif isinstance(value, str):
                            if i is 3:
                                # the case for the set_temporary has been initiated with path_only=True
                                intensive_mkdir(value, interface=self.msi)
                        #  arguments to manager
                        if kw in label:
                            mng.set_arg(label=label, args=value)
            managers.append(mng)
            self.logging('debug',
                         '[{}]-func_managers instance receives all required information.'.format(self.step_code),
                         method='_call_func_manager')
        return managers


class InterfaceBuilder(InterfaceHandler):
    """ Interface for wrapping shell commands or python function.
    Each interface is executed as single step node for the pipeline.
    For more detail on Interface plugin development, please see our tutorial.

    Methods:
        init_step:          initiate step, required the step title and code to prevent any conflict
                            with other step node. with type='python',
                            this step will run as the interface for the python function,
                            instead of the shell command.
        is_initiated:       check if the interface wrapper is initiated, useful when you working at the interpreter.
        set_input:          method to set input location
        set_output:         method to set output direction
        set_temporary:      method to set temporary file handler for running sub-step
        set_var:            method to set variable
        set_cmd:            method to set shell command (cannot use with set_func)
        set_func:           method to set python function (cannot use with set_cmd)
        set_errterm:        method to set error message string to indicate occurrence of the error event.
        set_output_checker: method to specify which filename to check after step execution.
                            error occurred if the output file are not found.
                            In case the output filename is different to the
                            input filename, the filename modifier must be provided.
        run:                method to schedule execution.

    """
    def __init__(self, processor: Processor, n_threads: int = None, relpath: bool = False):
        """
        Args:
            processor:      Processor instance
            n_threads:      number of threads
            relpath:        specify whether you are using relative path instead of absolute path on command
        Notes:
            relpath option added in response to the error related to the absolute path on AFNI's 3dttest++
        """
        super(InterfaceBuilder, self).__init__()
        self.reset(processor)

        if n_threads is None:
            self._n_threads = processor.scheduler_param['n_threads']
        else:
            self._n_threads = n_threads
        self._relpath = relpath
        self.logging('debug', f'n_threads={n_threads}, relpath={relpath}', method='__init__')
        # Initiate scheduler
        self._schd = Scheduler(n_threads=self._n_threads)

    def __enter__(self):
        """ in order to use the 'with' method """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def reset(self, processor: Optional[Processor] = None):
        """ reset all background process and clear queue """
        if processor is None:
            processor = self._procobj
        self._parse_info_from_processor(processor)
        self._init_attr_for_inspection()
        self._init_attr_for_execution()

        if self._schd is not None:
            self._schd = Scheduler(n_threads=self._n_threads)
        # self.__init__(self._procobj, n_threads=self._n_threads, relpath=self._relpath)

    @property
    def threads(self):
        """ return the scheduler object """
        return self._schd

    def init_step(self, title: str, suffix: Optional[str] = None,
                  idx: Optional[int] = None, subcode: Optional[str] = None,
                  mode='processing', type='cmd'):
        """ initiate step directory with unique step code to prevent any conflict on folder naming.
        Notes:
            in case of using same title, please use suffix to distinguish with other, which useful when
            repeating same command with different parameter set.
        Args:
            title:          the title for the step directory
            suffix:         suffix for the title
            idx:            index of step
            subcode:        sub-step code to identify the sub-step process
            mode:           'processing'- create step directory in working path
                            'reporting' - create step directory in result path
                            'masking'   - create step directory in mask path
            type:           'cmd'       - use command for processing data
                            'python'    - use python function for processing data
        """
        self.reset()
        if type not in ['cmd', 'python']:
            raise InvalidApproach('Invalid step type.')
        self._type = type
        run_order = self._update_run_order()
        # add current step code to the step list

        mode_dict = {'processing'   : 1,
                     'reporting'    : 2,
                     'masking'      : 3}

        if mode in mode_dict.keys():
            self._path = self._procobj.init_step(title=title, suffix=suffix,
                                                 idx=idx, subcode=subcode,
                                                 mode=mode)
            if self._relpath:
                self._path = os.path.relpath(self._path)
                self.logging('debug', f'using relative path: {self._path}', method=f'init_step-[{self.step_code}]')
            if self.step_code not in self._procobj.waiting_list:
                if self.step_code not in self._procobj.processed_list:
                    self._procobj.waiting_list.append(self.step_code)
                    self.logging('debug', 'added waiting list.',
                                 method=f'init_step-[{self.step_code}]')
                else:
                    # check if the current step had been processed properly
                    self._procobj.update()
                    base = {1: self._procobj.executed,
                            2: self._procobj.reported,
                            3: self._procobj.masked, }
                    if self.step_code in base[mode_dict[mode]].keys():
                        self.logging('debug',
                                     'has been processed, \n'
                                     'so this step will not be executed.',
                                     method='init_step-[{}]'.format(self.step_code))
                    else:
                        self._procobj.processed_list.remove(self.step_code)
                        self._procobj.waiting_list.append(self.step_code)
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

    def set_input(self, label: str, input_path: str, filter_dict: Optional[dict] = None,
                  group_input: bool = False, mask: bool = False,
                  idx: Optional[int] = None, join_modifier: Optional[dict] = None):
        """ method to set the input with filename filter, data can be collected from dataset path or working path,
        as well as masking path if mask is set to True.
        At least one input path need to be set for the interface.

        Notes:
            If you have multiple inputs, the total number of jobs are established from the first set of input
            and all other inputs must have the same number with first input. (inspection function will check it.)

            The group_input option can be used for the case that multiple inputs are required to get single output,
            such as group statistics. Since this form of processing can break the original data structure,
            only available when the mode of initiated step is 'reporting'.
            In this case, the input datasets will be collected according to the given filter_dict,
            then you can specify how to construct input string using join_modifier.
            e.g. If you set join_modifier as dict(prefix=None, suffix='[0]', spacer=', ')
                 then the input string will be
                 '/path/filename_1[0], /path/filename_2[0], ...'

        Args:
            label:          label for space holder on command template
            input_path:     absolute path, step directory, datatype or step code
            filter_dict:    filter set for parsing input data.
                            available keys={'contains', 'ignore', 'ext', 'regex', # for filename
                                            'subjects', 'sessions}                # for select specific group
            group_input:    False   - take one file for a input to run
                            True    - take multiple files as one input to run,
                                      only can be used for  'reporting' mode
            mask:           True if input is mask file
            idx:            index of the filtered dataset in order to pick one file as input
                            across subjects or sessions.
            join_modifier:  available option when group_input is True. Please check the Above Notes for detail.
                                    available keys={'prefix', 'suffix', # too add additional string on input paths
                                                    'spacer'}           # use given spacer between set of inputs
                                                                          (e.g. ',' or '\t') default is single space
        """
        run_order = self._update_run_order()
        # add current step code to the step list

        method = 1 if group_input else 0   # convert to legacy parameter
        daemon = self.get_daemon(self._set_input, run_order, label, input_path,
                                 filter_dict=filter_dict, method=method, mask=mask, idx=idx,
                                 join_modifier=join_modifier, relpath=self._relpath)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_static_input(self, label: str, input_path: str,
                         filter_dict: Optional[dict] = None,
                         idx: int = 0, mask: Optional[str] = False):
        """ method to set static input for each subject. useful when only one specific file need to be
        Args:
            label:          label for space holder on command template
            input_path:     absolute path, step directory, datatype or step code
            filter_dict:    filter set for parsing input data.
                            available keys={'contains', 'ignore', 'ext', 'regex', # for filename
                                            'subjects', 'sessions}                # for select specific group
            idx:            index of the filtered dataset in order to pick one file as input
                            across subjects or sessions.
            mask:           True if input is mask file
        """
        run_order = self._update_run_order()
        daemon = self.get_daemon(self._set_static_input, run_order, label, input_path,
                                 filter_dict=filter_dict, idx=idx, mask=mask, relpath=self._relpath)
        self._daemons[run_order] = daemon

    def set_output(self, label: str, prefix: Optional[str] = None,
                   suffix: Optional[str] = None, modifier: Optional[Union[dict, str]] = None,
                   ext: Optional[Union[str, bool]] = None):
        """This method will set output, if no input prior to this method, it will raise error.
        For the case of input methods 1 and 2, the output filename will be set as [subject]
        and [subject_session], respectively, and extension must be specified.

        Args:
            label:          output place-holder for command template,
                            'output' will help to prevent repetition of finished step
            prefix:         output filename prefix
            suffix:         output filename suffix
            modifier:       key(find):value(replace) or file(folder)name
                            in case the input method was set to 1,
                            user can specify file or folder name of output
            ext:            extension if it need to be changed. If False, extension will be removed.
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_output, run_order, label,
                                 modifier=modifier, ext=ext, prefix=prefix, suffix=suffix)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    @property
    def check_output(self):
        # this method is existing for backward compatibility.
        from shleeh.utils import deprecated_warning
        deprecated_warning('check_output', 'set_output_checker', future=True)
        return self.set_output_checker

    def set_output_checker(self, label: str = 'output',
                           prefix: Optional[str] = None,
                           suffix: Optional[str] = None,
                           ext: Optional[str] = None):
        """ method to generate output filter to check whether the output file has been generated,
        if the output file exists already, skip current step.

        Args:
            label:         main output placeholder on command template
            prefix:        in case the executing command add prefix to the output filename
            suffix:        in case the executing command add suffix to the output filename
            ext:           in case the executing command add extension to the output filename
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_output_checker, run_order, label,
                                 prefix=prefix, suffix=suffix, ext=ext)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_temporary(self, label: str, path_only: bool = False):
        """ method to set temporary output step.
        Args:
            label:          temporary output place-holder for command template.
            path_only:      create main step path only for
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_temporary, run_order, label, path_only, relpath=self._relpath)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_var(self, label: str, value: Any, quote: bool = False):
        """ method to set argument variables for function or shell command execution.
        Notes:
            If no input set prior to this method, Error will be raised.
        Args:
            label:          name of place-holder of variable for command of function template.
            value:          value to set as variable on command template
            quote:          True if the value need to be encapsulated by the quote on the shell command
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_var, run_order, label, value,
                                 quote=quote)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_cmd(self, command: str):
        """ method to set shell command, cannot use with 'set_func' method
        Notes:
            If no input set prior to this method, Error will be raised.
        Args:
            command:        command template, use decorator '*[label]' to place arguments.
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_cmd, run_order, command)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def set_func(self, func: Callable[..., bool]):
        """ method to set python function, cannot use with 'set_func' method
        Notes:
            if no input set prior to this method, Error will be raised.
        Args:
            func:           function template, the keyword argument on input
        """
        run_order = self._update_run_order()
        # add current step code to the step list
        daemon = self.get_daemon(self._set_func, run_order, func)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def run(self):
        """ schedule the execution
        Args:
            mode:           set 'python' if you use python function instead of shell command.
        """
        # submit job to scheduler
        run_order = self._update_run_order()
        # link this object to the parents class
        self._procobj.running_obj[self.step_code] = self
        # add current step code to the step list
        daemon = self.get_daemon(self._run, run_order)
        # update daemon to monitor
        self._daemons[run_order] = daemon

    def _run(self, run_order):
        """ hidden layer to run on daemon """
        if self._step_processed is True:
            pass
        else:
            # wait until previous command is finished.
            self._wait_my_turn(run_order, 'running interface command..', method='run')
            # command process start from here
            self._inspect_output()
            if self._type == 'python':
                self._mngs = self._call_func_manager()
            elif self._type == 'cmd':
                self._mngs = self._call_manager()
            else:
                raise InvalidApproach('Invalid step type.')
            for mng in self._mngs:
                try:
                    mng.schedule(self._schd, label=self.step_code)
                except TypeError:
                    self.logging('warn', 'TypeError occurred during job scheduling.',
                                 method='run-[{}]'.format(self.step_code))
                except:
                    self.logging('warn', 'UnexpectedError occurred during job scheduling.',
                                 method='run-[{}]'.format(self.step_code))
                    raise UnexpectedError
            self.logging('debug', 'processing scheduled.'.format(self.step_code),
                         method='run-[{}]'.format(self.step_code))
            self._schd.submit(mode='background', use_label=True)
            self._schd.join()  # because foreground option cannot check the status
            # command process end here

            inspect_result = self._inspect_run()
            # update dataset bucket
            self.logging('debug', 'updating dataset bucket.', method='run-[{}]'.format(self.step_code))
            self._bucket.update()

            # _parse stdout and stderr
            self.logging('debug', 'collect STDOUT from workers.', method='run-[{}]'.format(self.step_code))

            for label, workers in self._schd.stdout.items():
                for j in sorted(workers.keys()):
                    if re.search('_', label):
                        _, job_idx = label.split('_')
                    else:
                        job_idx = 0
                    if self._type == 'python':
                        mode_key = 'Func'
                        mode_val = self._schd.queues[int(job_idx)][j].func
                    else:
                        mode_key = 'CMD'
                        mode_val = self._schd.queues[int(job_idx)][j].cmd
                    if workers[j] is None:
                        self.logging('stdout', f'{mode_key}: {mode_val}\n   None\n')
                    else:
                        self.logging('stdout', '{0}: {1}\n  {2}'.format(mode_key, mode_val,
                                                                        '\n  '.join(workers[j])))
            self.logging('debug', 'collect STDERR from workers.', method='run-[{}]'.format(self.step_code))

            for label, workers in self._schd.stderr.items():
                for j in sorted(workers.keys()):
                    if re.search('_', label):
                        _, job_idx = label.split('_')
                    else:
                        job_idx = 0
                    if self._type == 'python':
                        mode_key = 'Func'
                        mode_val = self._schd.queues[int(job_idx)][j].func
                    else:
                        mode_key = 'CMD'
                        mode_val = self._schd.queues[int(job_idx)][j].cmd
                    if workers[j] is None:
                        self.logging('stderr', f'{mode_key}: {mode_val}\n   None\n')
                    else:
                        self.logging('stderr', '{0}: {1}\n  {2}'.format(mode_key, mode_val,
                                                                        '\n  '.join(workers[j])))

            if inspect_result:
                self.logging('warn', 'missing output file(s).', method='run-[{}]'.format(self.step_code))
            # step code update
            self.clear()
        # update executed folder
        self._procobj.update()

    @property
    def waiting_steps(self):
        """ return the step interface on waiting list for debugging """
        return self._procobj.waiting_list

    @property
    def processed_steps(self):
        """ return the step interface on processed """
        return self._procobj.processed_list

    def clear(self):
        """ clear step code assigned to current working step if it scheduled to the waiting list """
        if self.step_code is not None:
            last_step_code = self._procobj.waiting_list[0]
            if last_step_code != self.step_code:
                self.logging('warn', '** FATAL ERROR ** step code mismatch.',
                             method='run-[{}]'.format(self.step_code))
            else:
                self._procobj.processed_list.append(self._procobj.waiting_list.pop(0))
            self.logging('debug', 'processed.',
                         method='run-[{}]'.format(self.step_code))

    def get_inputs(self, label: str):
        """ the method to check assigned inputs, the input label must be specified
        Args:
            label:          input label
        """
        input_ready = False
        while input_ready is False:
            try:
                inputs = self._input_set[label]
                if isinstance(inputs, str):   # case of group_input is True
                    inputs = self._input_set[label].split(self._input_spacer)
                    input_ready = True
                elif isinstance(inputs, list):
                    input_ready = True
                return inputs
            except KeyError:  # label not in input_set, wait until it updated.
                time.sleep(self._refresh_rate)
        return

    def get_input_ref(self):
        """ return input reference (only used internally) """
        return self._input_ref

    def run_manually(self, args: dict):
        """ method to run current working step interface manually,
        Args:
            args:           the key:value pairs correspond to the argument you've set for this working step
            mode:           'python' if the working step interface is initiated for the python function
        """
        loop = True
        start = time.time()
        managers = []
        if self._type == 'python':
            while loop:
                time.sleep(self._refresh_rate)
                if len(self._func_set.keys()) == 0:
                    if time.time() - start < self._timeout:
                        pass
                    else:
                        raise NoFunction('[{}]-no func found'.format(self.step_code))
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
                time.sleep(self._refresh_rate)
                if len(self._cmd_set.keys()) == 0:
                    if time.time() - start < self._timeout:
                        pass
                    else:
                        raise NoCommand('[{}]-no command found'.format(self.step_code))
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
                raise UnexpectedError('[{}]-unexpected error occurred during job scheduling.'.format(self.step_code))

        print('[{}]-processing scheduled.'.format(self.step_code))
        self._schd.submit(use_label=True)

        for i, workers in self._schd.stdout.items():
            for j in sorted(workers.keys()):
                if workers[j] is None:
                    print('stdout', 'None\n')
                else:
                    print('stdout', '\n  {}'.format('\n  '.join(workers[j])))
        for i, workers in self._schd.stderr.items():
            for j in sorted(workers.keys()):
                if workers[j] is None:
                    print('stderr', 'None\n')
                else:
                    print('stderr', '\n  {}'.format(u'\n  '.join(workers[j])))
