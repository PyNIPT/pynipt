import os
import re
from paralexe import Manager, Scheduler

from ..core.base import config
from ..utils import *
refresh_rate = float(config.get('Preferences', 'daemon_refresh_rate'))


class InterfaceBuilder(object):
    """ The class for building a interface plugin

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
    def __init__(self, processor):
        self._procobj = processor
        self._schd = Scheduler(n_threads=processor.scheduler_param['n_threads'])
        self._msi = processor.msi
        self._bucket = self._procobj.bucket
        self._label = processor.label
        self._multi_session = self._bucket.is_multi_session()
        self._path = None                   # output path

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

        # attributes for controlling method execution
        # self._order_counter = 0
        # self._processed_run_order = []
        # self._daemons = dict()
        self._mngs = None

    # @property
    # def get_daemon(self):
    #     return self._procobj.get_daemon

    @property
    def msi(self):
        return self._msi

    def get_clone(self):
        if hasattr(self.msi, 'client'):
            clone_param = self.msi.client.clone()
            if clone_param is None:
                self.logging('warn', 'remote client cannot be cloned.', method='get_clone')
            else:
                Client, cfg = clone_param
                client = Client()
                client.connect(**cfg)
                return client
        else:
            return None

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

    # def _update_run_order(self):
    #     run_order = int(self._order_counter)
    #     self._order_counter += 1
    #     return run_order

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
        # run_order = self._update_run_order()
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
                    self.logging('debug', '[{}] is added waiting list.'.format(self.step_code), method='init_step')
                else:
                    self.logging('debug',
                                 '[{}] has been processed, \n'
                                 'restart the Processor class if you want to start over.'.format(self.step_code),
                                 method='init_step')
            else:
                self.logging('debug', '[{}] is added waiting list.'.format(self.step_code),
                             method='init_step')
        else:
            exc_msg = '[{}] is not an available mode.'.format(mode)
            self.logging('warn', exc_msg, method='init_step')

        self._init_step(mode_dict[mode])
        # daemon = self.get_daemon(self._init_step, run_order, mode_dict[mode])
        #
        # # update daemon to monitor
        # self._daemons[run_order] = daemon

    def _init_step(self, mode_idx): # run_order, mode_idx):
        """hidden method for init_step to run on threading"""
        # check if the init step is not first command user was executed

        # if run_order != 0:
        #     self.logging('warn', 'incorrect order, init_step must be the first method to be executed.',
        #                  method='init_step')
        # loop = True
        # while loop is True:
        #     if self.step_code == self._procobj._waiting_list[0]:
        #         loop = False

        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        self._procobj.bucket.update()
        self._procobj.update_attributes(mode_idx)
        # self._report_status(run_order)
        # if client is not None:
        #     client.close()

    # def _wait_my_turn(self, run_order, message=None, method=None):
    #     previous_run_order = run_order - 1
    #     loop = True
    #     while loop == True:
    #         if self.is_initiated():
    #             if previous_run_order in self._processed_run_order:
    #                 if not self._daemons[previous_run_order].is_alive():
    #                     loop = False
    #         time.sleep(refresh_rate)
    #     if message is not None:
    #         self.logging('debug', '[#{}]::{}'.format(str(run_order).zfill(3),
    #                                                   message), method=method)

    # def _report_status(self, run_order):
    #     self._processed_run_order.append(run_order)

    # def set_input(self, label, input_path, fname_filter=None, method=0, mask=False):
    #     """set the input with filename filter, data can be collected from dataset path or working path,
    #     as well as masking path if mask is set to True.
    #     At least one input path need to be set for building interface.
    #
    #     If there is multiple input, the total number of jobs will be established from first set of input
    #     and all other inputs must have the same number with first input. (inspection function will check it.)
    #
    #     Args:
    #         label(str):         label for space holder on command template
    #         input_path(str):    absolute path, step directory, datatype or step code
    #         fname_filter(dict): filename filter. available keys={'contains', 'ignore', 'ext', 'regex'}
    #         method (int):       0 - run command among all single files
    #                             1 - grouping it on subject level
    #                             2 - grouping it on session level if the dataset is multi-session
    #         mask(bool):         True if input is mask file
    #     Returns:
    #     """
    #     run_order = self._update_run_order()
    #     # add current step code to the step list
    #     daemon = self.get_daemon(self._set_input, run_order, label, input_path,
    #                              fname_filter=fname_filter, method=method, mask=mask)
    #     # update daemon to monitor
    #     self._daemons[run_order] = daemon

    # def set_output(self, label, modifier=None, ext=None):
    #     """method to set output, if no input prior to this method, this method will raise error.
    #     For input method cases 1 and 2, the output filename will be set as [subject]
    #     and [subject_session] respectively, this cases, extension need to be specified.
    #
    #     Args:
    #         label:          output place-holder for command template,
    #                         'output' will help to prevent repetition of finished step
    #         modifier(dict): key(find):value(replace)
    #         ext(str):       extension if it need to be changed. If False, extension will be removed.
    #     """
    #     run_order = self._update_run_order()
    #     # add current step code to the step list
    #     daemon = self.get_daemon(self._set_output, run_order, label,
    #                              modifier=modifier, ext=ext)
    #     # update daemon to monitor
    #     self._daemons[run_order] = daemon

    # def check_output(self, label='output', prefix=None, suffix=None, ext=None):
    #     """generate output filter to prevent execution if the output file is already exists.
    #
    #     Args:
    #         label(str):     main output placeholder on command template
    #         prefix(str):    in case the executing command add prefix to the output filename
    #         suffix(str):    in case the executing command add suffix to the output filename
    #         ext(str):       in case the executing command add extenrion to the output filename
    #     """
    #     run_order = self._update_run_order()
    #     # add current step code to the step list
    #     daemon = self.get_daemon(self._check_output, run_order, label,
    #                              prefix=prefix, suffix=suffix, ext=ext)
    #     # update daemon to monitor
    #     self._daemons[run_order] = daemon

    # def set_temporary(self, label):
    #     """method to set temporary output step. the structure of temporary folder
    #
    #     Args:
    #         label(str):     temporary output place-holder for command template.
    #     """
    #     run_order = self._update_run_order()
    #     # add current step code to the step list
    #     daemon = self.get_daemon(self._set_temporary, run_order, label)
    #     # update daemon to monitor
    #     self._daemons[run_order] = daemon
    #
    # def set_var(self, label, value, quote=False):
    #     """ If no input prior to this method, raise error
    #
    #     Args:
    #         label(str):                 place-holder of variable for command template.
    #         value(str, int, or list):   value to set as variable on command template
    #         quote(bool):                True if the value need to be encapsulated by the quote on command line
    #     """
    #     run_order = self._update_run_order()
    #     # add current step code to the step list
    #     daemon = self.get_daemon(self._set_var, run_order, label, value,
    #                              quote=quote)
    #     # update daemon to monitor
    #     self._daemons[run_order] = daemon
    #
    # def set_cmd(self, command):
    #     """If no input prior to this method, raise error
    #
    #     Args:
    #         command(str):   command template, use decorator '*[label]' to place arguments.
    #     """
    #     run_order = self._update_run_order()
    #     # add current step code to the step list
    #     daemon = self.get_daemon(self._set_cmd, run_order, command)
    #     # update daemon to monitor
    #     self._daemons[run_order] = daemon

    def set_input(self, label, input_path, fname_filter=None, method=0, mask=False):
        #  , run_order, label, input_path, fname_filter=None, method=0, mask=False):
        """hidden layer to run on daemon"""

        # self._wait_my_turn(run_order, '{}-{}'.format(label, input_path), method='set_input')

        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        method_name = 'set_input'
        num_inputset = len(self._input_set.keys())

        if num_inputset > 0:
            if self._input_method != method:
                exc_msg = 'input method is not match with prior set input(s).'
                self.logging('warn', exc_msg, method=method_name)
            else:
                if label in self._input_set.keys():
                    exc_msg = 'the label have been assigned already.'
                    self.logging('warn', exc_msg, method=method_name)
        else:
            self._input_method = method
            self._main_input = label

        input_path = self._procobj.inspect_input(input_path, mask=mask)
        if fname_filter is None:
            fname_filter = dict()
        else:
            exc_msg = 'insufficient filename filter is used.'
            if isinstance(fname_filter, dict):
                for key in fname_filter.keys():
                    if key not in self._bucket._fname_keys:
                        self.logging('warn', exc_msg, method=method_name)
            else:
                self.logging('warn', exc_msg, method=method_name)

        if self._input_method == 0:
            # use all single file as one input
            if input_path in self._bucket.params[0].datatypes:
                dset = self._bucket(0, datatypes=input_path, **fname_filter)
            else:
                if mask is True:
                    dset = self._bucket(3, datatypes=input_path, **fname_filter)
                else:
                    dset = self._bucket(1, pipelines=self._label, steps=input_path, **fname_filter)
            if num_inputset == 0:
                if self._multi_session is True:
                    self._input_ref = {i:(finfo.Subject, finfo.Session) for i, finfo in dset}
                else:
                    self._input_ref = {i:(finfo.Subject, None) for i, finfo in dset}
            self._input_set[label] = [finfo.Abspath for i, finfo in dset]

        else:
            if self._input_method == 1:

                def worker(idx):
                    self._procobj.update_attributes(idx)
                    for i, sub in enumerate(self._procobj.subjects):
                        if idx == 0 or idx == 3:
                            dset = self._bucket(idx, datatypes=input_path, subjects=sub, **fname_filter)
                        else:
                            dset = self._bucket(1, pipelines=self._label, steps=input_path,
                                                subjects=sub, **fname_filter)
                        if num_inputset == 0:
                            if self._multi_session is True:
                                ses = []
                                for j, finfo in dset:
                                    ses.append(finfo.Session)
                            else:
                                ses = None
                            self._input_ref[i] = (sub, ses)
                        self._input_set[label].append(' '.join([finfo.Abspath for i, finfo in dset]))

                # use all data in single subject as one input
                if num_inputset == 0:
                    self._input_ref = dict()
                self._input_set[label] = []
                if input_path in self._bucket.params[0].datatypes:
                    worker(0)
                else:
                    # mask as input
                    if mask is True:
                        worker(3)
                    # all other cases
                    else:
                        worker(1)

            elif self._input_method == 2:

                def worker(idx):
                    self._procobj.update_attributes(idx)
                    i = 0
                    for sub in self._procobj.subjects:
                        for ses in self._procobj.sessions:
                            if idx == 0 or idx == 3:
                                dset = self._bucket(idx, datatypes=input_path,
                                                    subjects=sub, sessions=ses,
                                                    **fname_filter)
                            else:
                                dset = self._bucket(idx, pipelines=self._label, step=input_path,
                                                    subjects=sub, sessions=ses,
                                                    **fname_filter)
                            if num_inputset == 0:
                                self._input_ref[i] = (sub, ses)
                            self._input_set[label].append('\s'.join([finfo.Abspath for i, finfo in dset]))
                            i += 1

                # use all data in single subject and single session as one input
                if self._multi_session is True:
                    if num_inputset == 0:
                        self._input_ref = dict()
                    self._input_set[label] = []
                    if input_path in self._bucket.params[0].datatypes:
                        worker(0)
                    else:
                        if mask is True:
                            worker(3)
                        else:
                            worker(1)
                else:
                    exc_msg = '[method=2] can only be used for multi-session data'
                    self.logging('warn', exc_msg, method=method_name)
            else:
                exc_msg = 'method selection is out of range.'
                self.logging('warn', exc_msg, method=method_name)
        # self._report_status(run_order)

        # if client is not None:
        #     client.close()

    def set_output(self, label, modifier=None, ext=None): # run_order, label, modifier, ext):
        """hidden layer to run on daemon"""
        # self._wait_my_turn(run_order, '{}'.format(label), method='set_output')

        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        method_name = 'set_output'
        input_name = self._main_input
        if self._main_input is None:
            exc_msg = 'Cannot find input set, run set_input method first.'
            self.logging('warn', exc_msg, method=method_name)
        else:
            self._inspect_label(label, method_name)

        def check_modifier(filename):
            if modifier is not None:
                if isinstance(modifier, dict):
                    for f, rep in modifier.items():
                        filename = change_fname(filename, f, rep)
                else:
                    exc_msg = 'wrong modifier!'
                    self.logging('warn', exc_msg, method=method_name)
            if ext is not None:
                if isinstance(ext, str):
                    filename = change_ext(filename, ext)
                elif ext == False:
                    filename = remove_ext(filename)
                else:
                    exc_msg = 'wrong extension!'
                    self.logging('warn', exc_msg, method=method_name)
            return filename

        # all possible input types, method 0, method 1, and method 2
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
            for i, (subj, _) in enumerate(self._input_ref.items()):
                filename = check_modifier(subj)
                self._output_set[label].append(self._path, filename)

        elif self._input_method == 2:
            for i, (subj, sess) in enumerate(self._input_ref.items()):
                filename = check_modifier('{}_{}'.format(subj, sess))
                self._output_set[label].append(self._path, filename)

        else:
            exc_msg = 'unexpected error, incorrect input_method.'
            self.logging('warn', exc_msg, method=method_name)

        # self._report_status(run_order)
        #
        # if client is not None:
        #     client.close()

    def check_output(self, label='output', prefix=None, suffix=None, ext=None): # run_order, label, prefix, suffix, ext):
        """hidden layer to run on daemon"""
        # self._wait_my_turn(run_order, '{}'.format(label), method='check_output')
        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        for l, v in self._output_set.items():
            if l == label:
                for p, fn in v:
                    if prefix is not None:
                        fn = '{}{}'.format(prefix, fn)
                    if suffix is not None:
                        fn = '{}{}'.format(fn, suffix)
                    if ext is not None:
                        fn = '{}.{}'.format(fn, ext)
                    self._output_filter.append((p, fn))

        if len(self._output_filter) == 0:
            self.logging('warn', 'insufficient information to generate output_filter.',
                         method='check_output')
        # self._report_status(run_order)
        #
        # if client is not None:
        #     client.close()

    def set_temporary(self, label): #run_order, label):
        """hidden layer to run on daemon"""
        # self._wait_my_turn(run_order, '{}'.format(label), method='set_temporary')
        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        method_name = 'set_temporary'
        input_name = self._main_input

        if self._main_input is None:
            exc_msg = 'Cannot find input set, run set_input method first.'
            self.logging('warn', exc_msg, method=method_name)
        else:
            self._inspect_label(label, method_name)

        step_path = self.msi.path.basename(self.path)
        temp_path = self._procobj.temp_path

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

        # self._report_status(run_order)

        # if client is not None:
        #     client.close()

    def set_var(self, label, value, quote=False): # run_order, label, value, quote):
        """hidden layer to run on daemon"""
        # self._wait_my_turn(run_order, '{}-{}'.format(label, value), method='set_var')
        method_name = 'set_var'
        self._inspect_label(label, method_name)
        if isinstance(value, list):
            if quote is True:
                value = ["'{}'".format(v) for v in value]
        elif isinstance(value, str) or isinstance(value, int):
            value = str(value)
            if quote is True:
                value = '"{}"'.format(value)
        else:
            exc_msg = 'incorrect variable.'
            self.logging('warn', exc_msg, method=method_name)
        self._var_set[label] = value
        # self._report_status(run_order)

    def set_cmd(self, command): #, run_order, command):
        """hidden layer to run on daemon"""
        # self._wait_my_turn(run_order, '{}'.format(command), method='set_cmd')
        self._cmd_set[len(self._cmd_set.keys())] = command
        # self._report_status(run_order)

    def _inspect_label(self, label, method_name=None):

        inspect_items = [self._input_set, self._output_set, self._var_set, self._temporary_set]
        for item in inspect_items:
            if label in item.keys():
                exc_msg = 'The label have been assigned already'
                self.logging('warn', exc_msg, method=method_name)

    # this function moved to the utils submodule
    # def _mk_dir(self, *abspath):
    #     for path in abspath:
    #         subpaths = []
    #         target_path, subpath = self.msi.path.split(path)
    #         subpaths.append(subpath)
    #         while not self.msi.path.exists(target_path):
    #             target_path, subpath = self.msi.path.split(target_path)
    #             subpaths.append(subpath)
    #         subpaths = subpaths[::-1]
    #         for subpath in subpaths:
    #             target_path = self.msi.path.join(target_path, subpath)
    #             if not self.msi.path.exists(target_path):
    #                 self.msi.mkdir(target_path)
    #
    # @staticmethod
    # def _rmv_ext(filename):
    #     pattern = re.compile(r'([^.]*)\..*')
    #     return pattern.sub(r'\1', filename)
    #
    # @staticmethod
    # def _chg_ext(filename, ext):
    #     if ext is False:
    #         return InterfaceBuilder._rmv_ext(filename)
    #     else:
    #         return '{}.{}'.format(InterfaceBuilder._rmv_ext(filename), ext)
    #
    # @staticmethod
    # def _chg_str(filename, find, replace):
    #     pattern = re.compile(r'^(.*){}(.*)$'.format(find))
    #     return pattern.sub(r'\1{}\2'.format(replace), filename)

    def logging(self, level, message, method=None):
        """The helper method to log message. If the logger is not initiated,
        only log the message if logger is initiated.

        Args:
            level (str): 'debug', 'stdout', or 'stderr'
            message (str): The message for logging.

        Raises:
            Exception: if level is not provided.
        """
        classname = 'InterfaceBuilder'
        if method is not None:
            message = '{}:{}-{}'.format(classname, method, message)
        else:
            message = '{}-{}'.format(classname, message)
        self._procobj.logging(level, message)
        if level == 'warn':
            raise Exception(message)

    def _parse_placeholder(self, manager, command):
        import re
        prefix, surfix = manager.decorator
        raw_prefix = ''.join([r'\{}'.format(chr) for chr in prefix])
        raw_surfix = ''.join([r'\{}'.format(chr) for chr in surfix])

        # The text
        p = re.compile(r"{0}[^{0}{1}]+{1}".format(raw_prefix, raw_surfix))
        return set([obj[len(prefix):-len(surfix)] for obj in p.findall(command)])

    def _inspect_output(self):
        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        msi = self.msi
        index_for_filter = []
        if len(self._output_filter):
            for i, (path, fname) in enumerate(self._output_filter):
                abspath = msi.path.join(path, fname)
                if msi.path.exists(abspath):
                    self.logging('debug', 'step [{}] is exists'.format(fname),
                                 method=self.step_code)
                    index_for_filter.append(i)
            if len(index_for_filter) > 0:
                self.logging('debug',
                             '[] of existing files detected, now excluding.'.format(len(index_for_filter)),
                             method='_inspect_output')
                arg_sets = [self._input_set, self._output_set, self._var_set, self._temporary_set]
                for arg_set in arg_sets:
                    for label, value in arg_set.items():
                        if isinstance(value, list):
                            arg_set[label] = [v for i, v in enumerate(value) if i not in index_for_filter]
            else:
                self.logging('debug', 'all outputs are passed the inspection.',
                             method=self.step_code)
        else:
            self.logging('debug', 'no output filter', method='_inspect_output')

        # if client is not None:
        #     client.close()

    def _call_manager(self):
        """call the Manager the command template and its arguments to Manager"""
        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        managers = []
        if len(self._cmd_set.keys()) == 0:
            self.logging('warn', 'there is no command', method='_call_manager')
        for i, cmd in sorted(self._cmd_set.items()):
            if hasattr(self.msi, 'client'):
                self.logging('debug', 'remote client detected.',
                             method='_call_manager')
                mng = Manager(self._procobj.bucket.msi.client)
            else:
                mng = Manager()
            placeholders = self._parse_placeholder(mng, cmd)
            self.logging('debug', 'placeholder in command template: [].'.format(placeholders),
                         method='_call_manager')
            mng.set_cmd(cmd)
            arg_sets = [self._input_set, self._output_set, self._var_set, self._temporary_set]
            for arg_set in arg_sets:
                for ph in placeholders:
                    for label, value in arg_set.items():
                        if isinstance(value, list):
                            if len(value) == 0:
                                return managers
                            else:
                                if isinstance(value[0], tuple):
                                    joined_values = []
                                    for v in value:
                                        intensive_mkdir(v[0], interface=self.msi)
                                        # self._mk_dir(v[0])
                                        joined_values.append(self.msi.path.join(*v))
                                    value = joined_values
                        if ph in label:
                            mng.set_arg(label=label, args=value)
            managers.append(mng)
            self.logging('debug', 'managers are got all information they need!',
                         method='_call_manager')
        #
        # if client is not None:
        #     client.close()
        return managers

    # def run(self):
    #     # submit job to scheduler
    #     run_order = self._update_run_order()
    #     # add current step code to the step list
    #     daemon = self.get_daemon(self._run, run_order)
    #     # update daemon to monitor
    #     self._daemons[run_order] = daemon

    def run(self): # , run_order):
        """hidden layer to run on daemon"""

        # Clear all other daemons
        # for i, d in self._daemons.items():
        #     if i != run_order:
        #         d.join()

        # client = self.get_clone()
        # if client is not None:
        #     self._procobj.bucket.msi = client.open_interface()

        # self._wait_my_turn(run_order, 'running interface command..', method='run') # wait until previous command is finished.

        # command process start from here
        self._inspect_output()
        self._mngs = self._call_manager()
        for mng in self._mngs:
            mng.schedule(self._schd, label=self.step_code)
        self.logging('debug', 'job scheduled for step::[{}] by the manager.'.format(self.step_code),
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
            self.logging('warn', 'something got wrong, stepcode missmatch, which can cause serious problem',
                         method='run')
        else:
            self._procobj._processed_list.append(self._procobj._waiting_list.pop(0))
        self.logging('debug', 'step_code is removed on the waiting list', method='run')
        #
        # if client is not None:
        #     client.close()

    # def summary(self): #TODO: print out summary
    #     pass


class PipelineBuilder(object):
    """ The class for building a pipeline plugin

    """
    def __init__(self, interface):
        self._interface = interface

    @property
    def interface(self):
        return self._interface

    @property
    def installed_packages(self):
        pipes = [pipe[5:] for pipe in dir(self) if 'pipe_' in pipe]
        output = dict(zip(range(len(pipes)), pipes))
        return output

