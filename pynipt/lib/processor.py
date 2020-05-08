import re
import logging
from collections import OrderedDict
from .bucket import BucketBase
from ..config import config
from ..errors import *

__dc__ = [config['Dataset structure'][c] for c in ['dataset_path', 'working_path', 'results_path',
                                                   'masking_path', 'temporary_path']]


class ProcessorBase(object):
    """The class provides the major functions to run pipeline.
    Base level of Interface class is separated for the purpose of maintenance.

    Notes:
        The Base level is oriented to interact with default properties setting.

    Args:
        bucket (:obj:'Bucket'): Bucket instance.
        label (str): The name of Pipeline package that this class will create.
        logger (bool): True for logging the whole processes.

    Methods:
        _get_step_dir: return the working step directory name of given step code
        _get_report_dir: return the result step directory name of given step code
        _parse_executed_subdir: return the step directory which contains files
        _parse_existing_subdir: return all the step directory even it empty
        update_attributes: update attributes from bucket.params
    """
    def __init__(self, bucket, label=None, logger=False):
        # private
        self._path      = None
        self._rpath     = None
        self._mpath     = None
        self._tpath     = None
        self._bucket    = None
        self._label     = None
        self._executed  = OrderedDict()
        self._reported  = OrderedDict()
        self._masked    = OrderedDict()
        self._tmp       = OrderedDict()
        # self._pattern   = re.compile(r'^(\d{2}[0A-Z]{1})_.*')
        self._pattern   = re.compile(r'^(\d{2}[0A-Z])_.*')

        # pre dataclass buffer
        self._pre_idx   = None

        # for debug
        self._log_path  = None
        self._logger    = None

        # check existing folders
        self._existing_step_dir     = dict()
        self._existing_report_dir   = dict()
        self._existing_mask_dir     = dict()
        self._existing_report_dir   = dict()

        # public
        self.msi = bucket.msi
        if isinstance(bucket, BucketBase):
            self._bucket = bucket
            if logger is True:
                self._init_logger()
                self.logging('debug', 'The Processor instance is initiated.')
        else:
            exc_msg = 'Invalid input argument.'
            self.logging('warn', exc_msg)
            raise InvalidInputArg(exc_msg)
        if label is not None:
            self.prepare_package_dir(label)

    def __del__(self):
        if self._logger is not None:
            self._disable_logger()

    @property
    def step_code_pattern(self):
        return self._pattern

    @property
    def path(self):
        return self._path

    @property
    def report_path(self):
        return self._rpath

    @property
    def mask_path(self):
        return self._mpath

    @property
    def temp_path(self):
        return self._tpath

    @property
    def bucket(self):
        return self._bucket

    @property
    def label(self):
        return self._label

    # setters
    @label.setter
    def label(self, label):
        if label is None:
            self.logging('warn', 'None object cannot be set to label.')
            raise NoneLabel
        else:
            self._label = label
            self.logging('debug', f'The name of the pipeline package is set as [{label}]')

    def _disable_logger(self):
        import logging
        from importlib import reload

        for handler_ in self._logger.handlers:
            self._logger.removeHandler(handler_)
        for filter_ in self._logger.filters:
            self._logger.removeFilter(filter_)
        self._logger.disabled = True
        logging.shutdown()
        reload(logging)

    def prepare_package_dir(self, label=None):
        """Internal level method to create pipeline package folder for selected dataclass"""

        # Inspect if label and dataclass is set-up
        if label is not None:
            self.label = label
            if self._logger is not None:
                self._disable_logger()
                self._init_logger()
        else:
            exc_msg = 'label is not properly set.'
            self.logging('warn', exc_msg)
            raise InvalidLabel(exc_msg)

        self._path  = self.msi.path.join(self.bucket.path, __dc__[1], self._label)
        self._rpath = self.msi.path.join(self.bucket.path, __dc__[2], self._label)
        self._mpath = self.msi.path.join(self.bucket.path, __dc__[3])
        self._tpath = self.msi.path.join(self.bucket.path, __dc__[4], self._label)

        # create working path only, result path creation will happen when reporting is initiated
        if not self.msi.path.exists(self._path):
            self.msi.mkdir(self._path)
            self.logging('debug', f'Folder:[{self._label}] is created.')

        if len(self.bucket) != 0:
            # update package path
            self.update_attributes(1)

    def logging(self, level: str, message: str):
        """method for logging the message,
        Args:
            level (str): 'debug', 'warn', 'stdout', or 'stderr'
            message (str): The message for logging.
        Raises:
            InvalidLoggingLevel: if level is not provided.
        """
        if self._logger is not None:
            if level == 'debug':
                self._logger.debug(message)
            elif level == 'warn':
                self._logger.warning(message)
            elif level == 'stdout':
                self._logger.info(message)
            elif level == 'stderr':
                self._logger.error(message)
            else:
                raise InvalidLoggingLevel(level)

    def _init_logger(self):
        """method for initiating logger"""
        self._log_path = self.msi.path.join(self.bucket.path, 'Logs')
        if not self.msi.path.exists(self._log_path):
            self.msi.mkdir(self._log_path)

        class LoggerFilter(object):
            def __init__(self, level: str):
                self._level = level

            def filter(self, log_record) -> bool:
                return log_record.levelno == self._level

        debug_fmt   = logging.Formatter('%(asctime)s ::%(levelname)s::[%(name)s] %(message)s')
        output_fmt  = logging.Formatter('%(asctime)s - %(message)s\n\n')

        if self._label is None:
            name = 'PreInit'
        else:
            name = self._label

        self._logger = logging.getLogger(name)

        debug_file  = self.msi.path.join(self._log_path, 'DEBUG.log')
        stdout_file = self.msi.path.join(self._log_path, 'STDOUT.log')
        stderr_file = self.msi.path.join(self._log_path, 'STDERR.log')

        handlers = []

        # # Below code originally designed for remote logging, but deprecated
        # References:
        # https: // stackoverflow.com / questions / 38907637 / quick - remote - logging - system
        # if hasattr(self.msi, 'client'):
        #     for f in [debug_file, debug_file, stdout_file, stderr_file]:
        #         handlers.append(logging.StreamHandler(self.msi.open(f, 'a')))
        # else:
        #     # For local logging
        for f in [debug_file, debug_file, stdout_file, stderr_file]:
            handlers.append(logging.FileHandler(f))

        def init_handlers(handler_, level, format):
            handler_.setLevel(level)
            handler_.addFilter(LoggerFilter(level))
            handler_.setFormatter(format)

        levels = [logging.DEBUG, logging.WARNING, logging.INFO, logging.ERROR]
        formats = [debug_fmt] * 2 + [output_fmt] * 2

        for i, handler in enumerate(handlers):
            init_handlers(handler, levels[i], formats[i])
            self._logger.addHandler(handler)
        # Main logger instance need to has maximal level to access all logging.
        self._logger.setLevel(logging.DEBUG)

    def update_attributes(self, idx):
        """The method to update attributes of the instance
        depends on selected dataclass. By applying this method, the subjects and sessions of the given
        indexed dataclass can be the attribute of this class.

        Args:
            idx (int): the index of dataset_path, working_path, or masks_path [0, 1, 3].

        Raises:
            IndexError: if given index is out of bound.
        """
        self._parse_existing_subdir()

        if idx in [0, 1, 3, 4]:
            pass
        else:
            exc_msg = f'Cannot parsing the attribute from [{__dc__[idx]}] class'
            self.logging('warn', exc_msg)
            raise IndexError(exc_msg)

        # clear previous attributes
        if self._pre_idx is not None:
            if idx != self._pre_idx:
                for k in self.bucket.param_keys[self._pre_idx]:
                    try:
                        delattr(self, k)
                    except AttributeError:
                        pass
            else:
                return

        # update attributes
        # self._avail_att = []
        keys = self.bucket.param_keys[idx]

        # the param_keys can be None if no data exists in the dataclass
        if keys is not None:
            self._pre_idx = idx
            # when processing dataclass is selected
            if idx == 1:
                # the first component in param_keys is name of pipeline
                filters = {keys[0]: self.label}

                # get dataset from current pipeline
                filtered = self.bucket(idx, **filters)

                # update attributes if filtered bucket has values
                if len(filtered) != 0:
                    columns = filtered.df.columns
                    for i, k in enumerate(keys):
                        setattr(self, k, self._sort_params(filtered.df[columns[i]]))
                else:
                    # If nothing, take information from dataset
                    dataset = self.bucket(0)
                    columns = dataset.df.columns
                    for i, k in enumerate(self.bucket.param_keys[0]):
                        setattr(self, k, self._sort_params(dataset.df[columns[i]]))
            else:
                self._pre_idx = idx
                dataset = self.bucket(idx)
                columns = dataset.df.columns
                for i, k in enumerate(self.bucket.param_keys[idx]):
                    setattr(self, k, self._sort_params(dataset.df[columns[i]]))
        else:
            # no data is found in given dataclass
            self._pre_idx = 0
            dataset = self.bucket(0)
            columns = dataset.df.columns
            for i, k in enumerate(self.bucket.param_keys[0]):
                setattr(self, k, self._sort_params(dataset.df[columns[i]]))

    def _parse_executed_subdir(self):
        """internal method to update subdir information which contains data."""
        base = {1: self._executed, 2: self._reported, 3: self._masked, 4: self._tmp}
        column_index = {1: 1, 2: 1, 3: 0, 4: 1}
        for i, dic in base.items():
            if self.bucket.param_keys[i] is not None:
                # if above is None, it's Empty bucket
                if i == 3:
                    filtered = self.bucket(i, copy=True)
                else:
                    if self.msi.path.exists(self.msi.path.join(self.bucket.path, __dc__[i], self.label)):
                        # check if the current package folder is created
                        filtered = self.bucket(i, self.label, copy=True)
                    else:
                        # if not, the result will be empty, no existing steps in this package
                        filtered = []

                if len(filtered) is 0:
                    dic.clear()
                else:
                    steps = self._sort_params(filtered.df[filtered.df.columns[column_index[i]]])

                    # Below loop will delete the step_code that not existing anymore.
                    orig_steplist = [f'{code}_{title}' for code, title in list(dic.items())]
                    bool_list = [s not in steps for s in orig_steplist]

                    for idx in list(filter(lambda x: bool_list[x], range(len(bool_list)))):
                        del dic[orig_steplist[idx].split('_')[0]]
                    for s in steps:
                        if i == 3:
                            n_files = len(self.bucket(i, datatypes=s))
                        elif i == 2:
                            n_files = len(self.bucket(i, pipelines=self.label, reports=s))
                        else:
                            n_files = len(self.bucket(i, pipelines=self.label, steps=s))
                        if n_files > 0:
                            dic[self._pattern.sub(r'\1', s)] = s[4:]

    def _parse_existing_subdir(self):
        """internal method to update all subdir information
        that created in each dataclass folders."""
        msi = self.msi
        steps = [d for d in msi.listdir(self.path) if msi.path.isdir(msi.path.join(self.path, d))]
        self._existing_step_dir = {self._pattern.sub(r'\1', s): s[4:] for s in steps}

        try:
            reports = [d for d in msi.listdir(self.report_path) if msi.path.isdir(msi.path.join(self.report_path, d))]
            self._existing_report_dir = {self._pattern.sub(r'\1', s): s[4:] for s in reports}
        except FileNotFoundError:
            self._existing_report_dir = dict()
        except:
            raise UnexpectedError

        try:
            masks = [d for d in msi.listdir(self.mask_path) if msi.path.isdir(msi.path.join(self.mask_path, d))]
            self._existing_mask_dir = {self._pattern.sub(r'\1', s): s[4:] for s in masks}
        except FileNotFoundError:
            self._existing_mask_dir = dict()
        except:
            raise UnexpectedError

        try:
            temps = [d for d in msi.listdir(self.temp_path) if msi.path.isdir(msi.path.join(self.temp_path, d))]
            self._existing_temp_dir = {self._pattern.sub(r'\1', s): s[4:] for s in temps}
        except FileNotFoundError:
            self._existing_temp_dir = dict()
        except:
            raise UnexpectedError

    def _get_step_dir(self, step_code, verbose=False):
        """return a directory name of working step path"""
        if step_code.upper() in self._existing_step_dir.keys():
            return f"{step_code}_{self._existing_step_dir[step_code]}"
        else:
            exc_msg = 'given step code is not exist.'
            if verbose:
                self.logging('warn', exc_msg)
            raise KeyError(exc_msg)

    def _get_temp_dir(self, step_code, verbose=False):
        """return a directory name of working step path"""
        if step_code.upper() in self._existing_temp_dir.keys():
            return f"{step_code}_{self._existing_temp_dir[step_code]}"
        else:
            exc_msg = 'given step code is not exist.'
            if verbose:
                self.logging('warn', exc_msg)
            raise KeyError(exc_msg)

    def _get_report_dir(self, step_code, verbose=False):
        """return directory name of result step path"""
        if step_code.upper() in self._existing_report_dir.keys():
            return f"{step_code}_{self._existing_report_dir[step_code]}"
        else:
            exc_msg = 'given report code is not exist.'
            if verbose:
                self.logging('warn', exc_msg)
            raise KeyError(exc_msg)

    def _get_mask_dir(self, step_code, verbose=False):
        """return directory name of marking step path"""
        if step_code.upper() in self._existing_mask_dir.keys():
            return "{}_{}".format(step_code, self._existing_mask_dir[step_code])
        else:
            exc_msg = 'given mask code is not exist.'
            if verbose:
                self.logging('warn', exc_msg)
            raise KeyError(exc_msg)

    @staticmethod
    def _sort_params(params):
        """The internal method to remove duplicated parameters
        and return the sorted values"""
        return sorted(list((set(params))))


class ProcessorHandler(ProcessorBase):
    """The class to handle Interface instance.

    Args:
        bucket (:obj:'Bucket'): Bucket instance
        label (str):

    Methods:
        inspect_input: check the integrity of input_path
        init_step: create a new step dir
        close_step: remove step dir if it empty
        clear: remove all empty step dir

    AAttributes:
        path (str): the absolute path for working directory path
        report_path (str): the absolute path for result directory path
        mask_path (str): the absolute path for mask directory path
        bucket: place holder for the bucket instance.
        label: the name of current allocated pipeline.
    """

    def __init__(self, *args, **kwargs):
        super(ProcessorHandler, self).__init__(*args, **kwargs)
        # self.update()

    def inspect_input(self, input_path, mask=False):
        """Check the integrity of input_path and return the correct form of input object.
        Notice: only dataset_path, working_path, and masking_path are available to use as input.

        Args:
            input_path (str): step code or absolute path of input. dtype if the input from data.
            mask (bool): True if the input is mask, in this case, the input_path must indicate the datatype of image.

        Returns:
            input_obj ('str' or 'tuple'):

        Raises:
            InspectionFailure
        """
        if isinstance(input_path, str):
            pattern = re.compile(r'\w{2}[0A-Z]')
            if mask is True:
                exc_msg = 'Cannot find mask data from given input_path.'
                try:
                    # can be absolute path if the given path has same data structure.
                    if self.msi.path.exists(input_path):
                        # return input_path
                        input_path = self.msi.path.basename(input_path)
                    # the exception will raised if no mask
                    elif input_path in set(self.bucket.params[3].datatypes):
                        pass
                    elif pattern.match(input_path.upper()):
                        # return self.msi.path.join(self.bucket.path, dc[3], self._get_mask_dir(input_path.upper())
                        input_path = self._get_mask_dir(input_path.upper())
                    else:
                        raise UnexpectedError
                except:
                    self.logging('warn', exc_msg)
                    raise InspectionFailure(exc_msg)
            else:
                exc_msg = 'Cannot find the datatype mapped with input_path.'
                try:
                    if self.msi.path.exists(input_path):
                        # return input_path
                        input_path = self.msi.path.basename(input_path)
                    elif input_path in set(self.bucket.params[0].datatypes):
                        # return self.msi.path.join(self.bucket.path, dc[0]), input_path
                        pass
                    elif pattern.match(input_path.upper()):
                        # return self.msi.path.join(self.path, self._get_step_dir(input_path.upper()))
                        input_path = self._get_step_dir(input_path.upper())
                    else:
                        raise UnexpectedError
                except:
                    self.logging('warn', exc_msg)
                    raise InspectionFailure(exc_msg)
        else:
            exc_msg = 'The given input cannot pass the inspection.'
            self.logging('warn', exc_msg)
            raise InspectionFailure(exc_msg)

        return input_path

    @staticmethod
    def _split_step_code(step_code):
        """split step code into index and substep_code

        Args:
            step_code: first three character of step dir name

        Returns:
            step_idx (int): step index
            substep_code (None or [A-Z]): if the code is 0, return None, else return one of capital Alphabet

        """
        step_idx = int(step_code[:2])
        substep_code = step_code[-1]

        try:
            if int(substep_code) == 0:
                substep_code = None
        except ValueError:
            pass
        return step_idx, substep_code

    def init_step(self, title, mode='processing', suffix=None, idx=None, subcode=None):
        """create a new step directory on selected mode

        Args:
            title (str): step title.
            mode (str): ['processing', 'reporting', 'masking']
            suffix (str): suffix need to be added to title.
            idx (int): index of the step
            subcode (int or str): the code with 0 or A-Z to indicate sub-step order.

        Returns:
            abspath (str): the absolute path of initiated step.
        """
        # prepare all available sub-step codes and executed steps information
        import string
        avail_codes = string.ascii_uppercase

        if mode in ['processing', 'reporting', 'masking']:
            existing_dir = dict(list(self._existing_step_dir.items())
                                + list(self._existing_mask_dir.items())
                                + list(self._existing_report_dir.items()))
        else:
            exc_msg = '[{}] is not available mode.'.format(mode)
            self.logging('warn', exc_msg)
            raise InvalidMode(exc_msg)

        # inspect idx and subcode
        if idx is not None:
            if isinstance(idx, int) and (idx < 99) and (idx > 0):
                pass
            else:
                exc_msg = 'The given step index is out of range (0 - 99).'
                self.logging('warn', exc_msg)
                raise InvalidStepCode(exc_msg)
        if subcode is not None:
            if isinstance(subcode, str):
                subcode = subcode.upper()
            if (subcode == 0) or str(subcode) in avail_codes:
                pass
            else:
                exc_msg = 'The given sub-step code is out of range (0 or A-Z).'
                self.logging('warn', exc_msg)
                raise InvalidStepCode(exc_msg)

        # update suffix to title
        if suffix is not None:
            title = "{}-{}".format(title, suffix)

        # if no folder created so far
        if len(existing_dir.keys()) == 0:
            if subcode is None:
                subcode = 0
            if idx is None:
                # the code for the very first step will be 010
                new_step_code = f'01{subcode}'
            else:
                # if idx are given, then use it
                new_step_code = f"{str(idx).zfill(2)}{subcode}"
        else:
            # parse step code from list of executed steps
            existing_codes = sorted(existing_dir.keys())
            existing_titles = [existing_dir[c] for c in existing_codes]

            # check if the same title have been used
            duplicated_title_idx = [i for i, t in enumerate(existing_titles) if title == t]
            if len(duplicated_title_idx) != 0:
                # since will not allow duplicated title, it will be not over 1
                if idx is not None:
                    exc_msg = 'Duplicated title, please use suffix to make it distinct.'
                    if idx == int(existing_codes[duplicated_title_idx[0]][:2]):
                        new_step_code = existing_codes[duplicated_title_idx[0]]
                        if subcode is not None:
                            if new_step_code[-1] != str(subcode):
                                self.logging('warn', exc_msg)
                                raise Duplicated(exc_msg)
                    else:
                        self.logging('warn', exc_msg)
                        raise Duplicated(exc_msg)
                else:
                    new_step_code = existing_codes[duplicated_title_idx[0]]

            else:
                # there is no duplicated title
                existing_step_idx = [int(self._split_step_code(c)[0]) for c in existing_codes]
                existing_substep_code = [self._split_step_code(c)[1] for c in existing_codes]

                if idx is not None:
                    # check the executed step index if it previously used
                    duplicated_idx_steps = [i for i, esi in enumerate(existing_step_idx) if esi == idx]
                    new_step_idx = idx
                    if len(duplicated_idx_steps) == 0:
                        # there is no duplicated index
                        if subcode is not None:
                            new_substep_code = subcode
                        else:
                            new_substep_code = 0
                    else:
                        # there is duplicated index, check the sub-step code
                        if subcode is not None:
                            new_substep_code = subcode
                        else:
                            latest_substep_code = existing_substep_code[max(duplicated_idx_steps)]
                            if latest_substep_code is not None:
                                new_substep_code_idx = avail_codes.find(latest_substep_code) + 1
                                if new_substep_code_idx >= len(avail_codes):
                                    # The number of allowable sub-step code limited between 0 and A-Z
                                    exc_msg = 'The sub-step code is exceed its limit.'
                                    self.logging('warn', exc_msg)
                                    raise InvalidStepCode(exc_msg)
                                new_substep_code = avail_codes[new_substep_code_idx]
                            else:
                                new_substep_code = avail_codes[0]
                else:
                    # idx was not given, generate new one
                    if subcode is not None:
                        # subcode must use with idx
                        exc_msg = 'not allowed to use sub-step code without idx argument.'
                        self.logging('warn', exc_msg)
                        raise InvalidStepCode(exc_msg)

                    new_substep_code = 0
                    new_step_idx = max(existing_step_idx) + 1

                # compose step code using idx and substep code
                new_step_code = "{}{}".format(str(new_step_idx).zfill(2), new_substep_code)
                if new_step_code in existing_codes:
                    if title not in existing_titles:
                        exc_msg = ['the step code had been used already.\n',
                                   f'current_title: {title}',
                                   'existing_title:']
                        for t in existing_titles:
                            exc_msg.append(f'\t{t}')
                        self.logging('warn', '\n'.join(exc_msg))
                        raise InvalidStepCode('\n'.join(exc_msg))

        # dir name
        new_step_dir = "{}_{}".format(new_step_code, title)

        if mode is 'processing':
            abspath = self.msi.path.join(self.path, new_step_dir)
        elif mode is 'reporting':
            if not self.msi.path.exists(self.report_path):
                self.msi.mkdir(self.report_path)
                self.logging('debug', f'Folder:[{self.label}] is created on [{__dc__[2]}] class')
            abspath = self.msi.path.join(self.report_path, new_step_dir)
        elif mode is 'masking':
            if not self.msi.path.exists(self.mask_path):
                self.msi.mkdir(self.mask_path)
                self.logging('debug', f'Folder:[{self.label}] is created on [{__dc__[2]}] class')
            abspath = self.msi.path.join(self.mask_path, new_step_dir)
        else:
            exc_msg = '[{}] is not available mode.'.format(mode)
            self.logging('warn', exc_msg)
            raise InvalidMode(exc_msg)

        if not self.msi.path.exists(abspath):
            self.msi.mkdir(abspath)
            self.logging('debug', f'[{new_step_dir}] folder is created.')
        else:
            self.logging('debug', f'[{new_step_dir}] folder is already exist.')
        self._parse_existing_subdir()
        return abspath

    def close_step(self, step_code, mode='processing'):
        """delete the step folder if folder is empty.

        Args:
            step_code (str): first three character of step dir name
            mode (str): ['processing', 'reporting']
        """
        if mode is 'processing':
            step_dir = self._get_step_dir(step_code)
            step_path = self.msi.path.join(self.path,
                                           step_dir)
        elif mode is 'reporting':
            step_dir = self._get_report_dir(step_code)
            step_path = self.msi.path.join(self.report_path,
                                           step_dir)
        else:
            exc_msg = f'[{mode}] is not available mode.'
            self.logging('warn', exc_msg)
            raise InvalidMode(exc_msg)

        if self.msi.path.exists(step_path):
            if len(self.msi.listdir(step_path)) == 0:
                self.msi.rmdir(step_path)
                self.logging('debug', f'[{step_dir}] folder is deleted.')
            else:
                pass
        self.update()

    def clear(self):
        """clean all empty folders
        """
        for step_code in self._existing_step_dir.keys():
            self.close_step(step_code, mode='processing')

        for step_code in self._existing_report_dir.keys():
            self.close_step(step_code, mode='reporting')

    def destroy_step(self, step_code, mode='processing'):
        """delete the step folder if folder is empty.

                Args:
                    step_code (str): first three character of step dir name
                    mode (str): ['processing', 'reporting']
                """
        if mode is 'processing':
            step_dir = self._get_step_dir(step_code)
            step_path = self.msi.path.join(self.path,
                                           step_dir)
        elif mode is 'reporting':
            step_dir = self._get_report_dir(step_code)
            step_path = self.msi.path.join(self.report_path,
                                           step_dir)
        elif mode is 'masking':
            step_dir = self._get_mask_dir(step_code)
            step_path = self.msi.path.join(self.mask_path,
                                           step_dir)
        else:
            exc_msg = f'[{mode}] is not available value for the mode.'
            self.logging('warn', exc_msg)
            raise InvalidMode(exc_msg)

        if self.msi.path.exists(step_path):
            if len(self.msi.listdir(step_path)) == 0:
                self.msi.rmdir(step_path)
                self.logging('debug', f'[{step_dir}] folder is deleted.')
            else:
                import shutil
                shutil.rmtree(step_path)
                self.logging('debug', f'[{step_dir}] folder contained data, but now it is destroyed.')
        self.update()

    def update(self):
        self.bucket.update()
        self._parse_existing_subdir()
        self._parse_executed_subdir()


class Processor(ProcessorHandler):
    """The class has a role to interface between Scheduler and Manager
    If the process methods defined in this class is serially operated,
    all jobs will scheduled on scheduler and operate as single pipeline

    PyNIPT uses the home-designed data structure, we named is as 'Project folder',
    based on a BIDS standard <http://bids.neuroimgaing.io> for enabling to manage
    the set of the derivatives more efficiently.

    If you want more detail information, please refer our documentation in github.

    Args:
        bucket (:obj:'Bucket'):     Bucket instance.
        label (str):                The name of Pipeline package that this class will create.
        logger (bool):              True for logging the whole processes.


    Methods:
        get_daemon:                 get daemon for threading
        inspect_input:              check the integrity of input_path
        init_step:                  create a new step dir
        close_step:                 remove step dir if it empty
        clear:                      remove all empty step dir

    Attributes:
        scheduler_param:            scheduler config parameters
        summary:                    print out the summary of processor instance

    """
    def __init__(self, *args, **kwargs):
        cfg = config['Preferences']
        if 'n_threads' in kwargs.keys():
            if kwargs['n_threads'] is None:
                self._n_threads = cfg.getint('number_of_threads')
            else:
                self._n_threads = kwargs.pop('n_threads')
        else:
            self._n_threads = cfg.getint('number_of_threads')
        super(Processor, self).__init__(*args, **kwargs)

        # install default interface in plugin folder
        # to control scheduling issues,
        self._waiting_list = []
        self._processed_list = []
        self._running_obj = OrderedDict()
        self.update()

    @property
    def waiting_list(self):
        return self._waiting_list

    @property
    def processed_list(self):
        return self._processed_list

    @property
    def executed(self):
        return self._executed

    @property
    def reported(self):
        return self._reported

    @property
    def masked(self):
        return self._masked

    @property
    def running_obj(self):
        """This property is the placeholder to save InterfaceBuilder instance.
        """
        return self._running_obj

    @property
    def scheduler_param(self):
        """The parameters will be taken by Scheduler class. """
        return dict(queue=self._waiting_list,
                    done=self._processed_list,
                    n_threads=self._n_threads)

    @property
    def get_step_dir(self):
        return self._get_step_dir

    @property
    def get_mask_dir(self):
        return self._get_mask_dir

    @property
    def get_report_dir(self):
        return self._get_report_dir

    @staticmethod
    def get_daemon(func, *args, **kwargs):
        """Generate daemon for scheduling internal processing step for interface job"""
        import threading
        daemon = threading.Thread(target=func, args=args, kwargs=kwargs)
        daemon.daemon = True
        daemon.start()
        return daemon

    def __repr__(self):
        return self.summary

    @property
    def summary(self):
        return str(self._summary())

    def _summary(self):
        """ Print summary information about the instance derived from this class.

        Returns:
            summary (str): summary information of the instance

        """
        s = list()
        s.append(f"** Summary of Processor instance initiated for [{self.label}].\n")
        s.append(f"- Abspath of initiated package:\n\t{self.path}")
        s.append(f"- The base dataclass of updated attributes:\n\t{__dc__[self._pre_idx]}")
        s.append(f"- Available attributes:\n\t{self.bucket.param_keys[self._pre_idx]}")
        if len(self._executed) is 0:
            pass
        else:
            s.append("- Processed steps:")
            for i, step in self._executed.items():
                s.append(f"\t{i}: {step}")
        if len(self._reported) is 0:
            pass
        else:
            s.append("- Reported steps:")
            for i, step in self._reported.items():
                s.append(f"\t{i}: {step}")
        if len(self._masked) is 0:
            pass
        else:
            s.append("- Masked data:")
            for i, step in self._masked.items():
                s.append(f"\t{i}: {step}")
        output = '\n'.join(s)
        return output
