#%% import modules
import os
import re
import time
import sys
import logging
from collections import namedtuple, OrderedDict
if sys.version_info[0] == 3:
    # import configparser
    from importlib import reload
else:
    # import ConfigParser as configparser
    from imp import reload
from .config import config


dataclasses = ['dataset_path', 'working_path', 'results_path', 'masking_path', 'temporary_path']
dc = [config.get('Dataset structure', c) for c in dataclasses]
ignore = config.get('Dataset structure', 'ignore').split(',')
_refresh_rate = float(config.get('Preferences', 'daemon_refresh_rate'))


#%% Dataset base class
class BucketBase(object):
    """The class to navigate the dataset information.

    The role of this class is parse the data structure from data folder,
    and keep it as properties to be utilized by other class.

    Notes:
        The Base level is oriented to interact with default properties setting.
        Particularly, scan the folder and parse the data structure.

    Attributes:
        path (str): absolute path of dataset.
        columns (dict): columns for each dataclass
        param_keys (dict): the set of keys to indicate item to filter
        params (dict): the set of parameters which can be used for the filter for each dataclass
    """
    def __init__(self):
        self.__initiate_base_attributes()

    def __initiate_base_attributes(self):
        self._path = None
        self._dataset = dict()
        self._column_info = dict()
        self._param_keys = dict()
        self._params = dict()
        self._multi_session = False
        self.msi = None

    @property
    def df(self):
        from pandas import DataFrame
        return DataFrame

    @property
    def columns(self):
        return self._column_info

    @property
    def path(self):
        return self._path

    @property
    def params(self):
        return self._params

    @property
    def param_keys(self):
        return self._param_keys

    def set_path(self, path):
        self._path = self._abspath(path)

    def _walk(self, path):
        return tuple((None, None, None))

    def _abspath(self, path):
        return self.msi.path.abspath(path)

    def compose_columns(self, idx, max_depth):
        """This method checks the properness of given dataclass index and maximum depth.

        Args:
            idx:
            max_depth:

        Returns:
            columns (list) if successful, False otherwise.

        Raises:
            IndexError if idx out of bound.
        """
        columns = ['Subject', 'Filename', 'Abspath']
        if max_depth == 0:
            return False
        if idx == 0:    # dataset folder
            columns.insert(1, 'Datatype')
            if max_depth == 3:      # multi session case
                self._multi_session = True
                columns.insert(1, 'Session')
            elif max_depth == 2:    # single session case
                pass
            else:                   # otherwise, response as empty
                return False

        elif idx == 1 or idx == 4:  # working folder
            if max_depth == 4:      # multi session case
                self._multi_session = True
                columns.insert(1, 'Session')
            elif max_depth == 3:    # single session case
                pass
            else:                   # otherwise, response as empty
                return False
            columns = ['Pipeline', 'Step'] + columns

        elif idx == 2:  # result folder
            if max_depth >= 2:      # if contains any output
                columns = ['Pipeline', 'Report', 'Output'] + columns[2:]
            else:
                return False        # otherwise, response as empty

        elif idx == 3:  # mask folder
            if max_depth == 3:      # multi session case
                self._multi_session = True
                columns.insert(1, 'Session')
                columns.insert(0, 'Datatype')
            elif max_depth == 2:    # single session case
                columns.insert(0, 'Datatype')
            else:                   # otherwise, response as empty
                return False

        # index out of bound
        else:
            raise IndexError
        return columns

    def parser(self, path):
        """This method checks the maximum depth of the given path,
        and return the file list in the deepest location

        This method will work both for remote and local buckets.

        Args:
            path (str):

        Returns:
            container (dict):
            max_depth (int):
        """
        # convert to absolute path
        path = self._abspath(path)
        input_length = len(path.split(self.msi.sep))

        # scan all data in the path
        data_tree = self._walk(path)
        max_depth = 0
        container = dict()

        # data_tree will iter three components
        for abs_path, sub_dirs, sub_files in data_tree:
            path_comp = abs_path.split(self.msi.sep)[input_length:]
            cur_depth = len(path_comp)

            # new depth added
            if cur_depth not in container.keys():
                container[cur_depth] = dict()

            # prepare the component
            comp_id = len(container[cur_depth].keys())
            if comp_id not in container[cur_depth].keys():
                container[cur_depth][comp_id] = dict()
                container[cur_depth][comp_id]['path_comp'] = list()
                container[cur_depth][comp_id]['sub_files'] = list()
                container[cur_depth][comp_id]['sub_dirs'] = list()

            if max_depth < cur_depth:
                max_depth = cur_depth
            container[cur_depth][comp_id]['path_comp'].append(path_comp)
            container[cur_depth][comp_id]['sub_files'].append(sub_files)
            container[cur_depth][comp_id]['sub_dirs'].append(sub_dirs)
        return self._inspect_container(container, max_depth)

    def _inspect_container(self, container, max_depth):
        """Filter out the containers only having empty folder.
        Args:
            container (dict):
            max_depth (int):

        Returns:
            container (dict):
            max_depth (int):
        """
        # If the deepest folder does not contains any files
        if max_depth == 0:
            pass
        else:
            counter = 0
            for comp in container[max_depth].values():
                for f in comp['sub_files']:
                    counter += len(f)

            if counter == 0:
                del (container[max_depth])
                max_depth -= 1
        return container, max_depth

    def scan(self, idx):
        """the method to scan data structure of selected dataclass.

        Notes:
            Using 'parser' methods, this method collect the maximal depth
            and collect all file information of the dataclass folder.
            This method is designed to work in both remote and local buckets.

        Args:
            idx (int): the index of the dataclass.

        Returns:
            True if successful, False otherwise.
        """
        path = self.msi.path.join(self._path, dc[idx])
        container, max_depth = self.parser(path)
        columns = self.compose_columns(idx, max_depth)

        # if the folder does not contain any data
        if columns is False:
            self._dataset[idx] = None
            self._column_info[idx] = None
            self._param_keys[idx] = None
            self._params[idx] = None
            return False
        else:
            finfo = namedtuple('finfo', columns)
            param_keys = ["{}s".format(col.lower()) for col in columns[:-2]]
            param_dict = {k: [] for k in param_keys}
            param = namedtuple('param', param_keys)

            # result cases
            if idx == 2:
                try:
                    # The report data could lose the file structure constancy,
                    # and can be the multiple files or folder, so that not easy to handle the cases.
                    container = container[2]
                    iter_obj = []
                    for comp in container.values():
                        # Due to the above reasons, at the maximum depth, both files
                        # and folders treated as the report components.
                        iter_obj.extend(comp['sub_files'] + comp['sub_dirs'])
                except:
                    return False

            # processing cases
            else:
                container = container[max_depth]
                iter_obj = []
                for comp in container.values():
                    iter_obj.extend(comp['sub_files'])

            list_finfo = []

            i = 0
            for j in container:
                for components in container[j]['path_comp']:
                    basepath = self.msi.path.join(path, *components)
                    for p, comp in enumerate(components):
                        param_dict[param_keys[p]].append(comp)
                        param_dict[param_keys[p]] = sorted(list(set(param_dict[param_keys[p]])))
                    if idx == 2:
                        items = container[j]['sub_dirs'] + container[j]['sub_files']
                    else:
                        items = container[j]['sub_files']
                    for _ in items:
                        if len(iter_obj[i]) > 0:
                            for f in iter_obj[i]:
                                if f in ignore:
                                    pass
                                else:
                                    abspath = self.msi.path.join(basepath, f)
                                    list_finfo.append(finfo(**dict(zip(columns, components + [f, abspath]))))
                        i += 1

            self._dataset[idx] = list_finfo
            self._column_info[idx] = columns
            self._param_keys[idx] = param_keys
            self._params[idx] = param(**param_dict)
        return True

    def update(self, idx=None):
        """This method performs scan method on given dataclass or all dataclasses.

        Args:
            idx (int): index number for dataclass of interest.

        Returns:
            True if successful, False otherwise.
            In case the idx value is not given, return list of True and False according to each idx.

        Raises:
            IndexError if idx is out of bound.
        """
        if idx is None:
            resp = list()
            for i in range(len(dc)):
                resp.append(self.scan(i))
        elif idx in range(len(dc)):
            resp = self.scan(idx)
        # index out of bound
        else:
            raise IndexError
        return resp

    def __call__(self, *args, **kwargs):
        return self

    def __len__(self):
        return 0


#%% Interface base class
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

    Attributes:
        path (str): the absolute path for working directory path
        report_path (str): the absolute path for result directory path
        mask_path (str): the absolute path for mask directory path
        bucket: place holder for the bucket instance.
        label: the name of current allocated pipeline.
    """
    def __init__(self, bucket, label=None, logger=False):
        self.__initiate_base_attributes()
        self.msi = bucket.msi
        if isinstance(bucket, BucketBase):
            self._bucket = bucket
            if logger is True:
                self._init_logger()
                self.logging('debug', 'The Processor instance is initiated.')
        else:
            exc_msg = 'Wrong bucket object is given.'
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)
        if label is not None:
            self.prepare_package_dir(label)

    def __del__(self):
        if self._logger is not None:
            self._disable_logger()

    def __initiate_base_attributes(self):
        from collections import OrderedDict
        self._path = None
        self._rpath = None
        self._mpath = None
        self._tpath = None
        self._bucket = None
        self._label = None
        self._executed = OrderedDict()
        self._reported = OrderedDict()
        self._masked = OrderedDict()
        self._tmp = OrderedDict()
        self._pattern = re.compile(r'^(\d{2}[0A-Z]{1})_.*')
        # self._avail_att = list()

        # pre dataclass buffer
        self._pre_idx = None

        # for debug
        self._log_path = None
        self._logger = None

        # check existing folders
        self._existing_step_dir = dict()
        self._existing_report_dir = dict()
        self._existing_mask_dir = dict()
        self._existing_report_dir = dict()

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
            exc_msg = 'Label cannot be set to None'
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)
        else:
            self._label = label
            self.logging('debug', 'Label is set to [{}]'.format(label))

    def _disable_logger(self):
        for handler in self._logger.handlers:
            self._logger.removeHandler(handler)
        for filter in self._logger.filters:
            self._logger.removeFilter(filter)
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
            raise Exception(exc_msg)

        self._path = self.msi.path.join(self.bucket.path,
                                        dc[1],
                                        self._label)
        self._rpath = self.msi.path.join(self.bucket.path,
                                         dc[2],
                                         self._label)
        self._mpath = self.msi.path.join(self.bucket.path,
                                         dc[3])
        self._tpath = self.msi.path.join(self.bucket.path,
                                         dc[4],
                                         self._label)

        # create working path only, result path creation will happen when reporting is initiated
        if not self.msi.path.exists(self._path):
            self.msi.mkdir(self._path)
            self.logging('debug', 'Folder:[{}] is created.'.format(self._label))

        if len(self.bucket) != 0:
            # update package path
            self.update_attributes(1)

    def logging(self, level, message):
        """The helper method to log message. Only log the message if logger is initiated.

        Args:
            level (str): 'debug', 'stdout', or 'stderr'
            message (str): The message for logging.

        Raises:
            Exception: if level is not provided.
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
                raise Exception('[{}] is not available level for logging.'.format(level))

    def _init_logger(self):
        """Internal method to initiate logger instance. If the bucket is remote system use remote log.

        References:
            https://stackoverflow.com/questions/38907637/quick-remote-logging-system
        """
        self._log_path = self.msi.path.join(self.bucket.path, 'Logs')
        if not self.msi.path.exists(self._log_path):
            self.msi.mkdir(self._log_path)

        class LoggerFilter(object):
            def __init__(self, level):
                self.__level = level

            def filter(self, logRecord):
                return logRecord.levelno == self.__level

        debug_fmt = logging.Formatter('%(asctime)s ::%(levelname)s::[%(name)s] %(message)s')
        output_fmt = logging.Formatter('%(asctime)s - %(message)s\n\n')

        if self._label is None:
            name = 'PreInit'
        else:
            name = self._label

        self._logger = logging.getLogger(name)

        debug_file = self.msi.path.join(self._log_path, 'DEBUG.log')
        stdout_file = self.msi.path.join(self._log_path, 'STDOUT.log')
        stderr_file = self.msi.path.join(self._log_path, 'STDERR.log')

        handlers = []

        if hasattr(self.msi, 'client'):
            for f in [debug_file, debug_file, stdout_file, stderr_file]:
                handlers.append(logging.StreamHandler(self.msi.open(f, 'a')))
        else:
            # For local logging
            for f in [debug_file, debug_file, stdout_file, stderr_file]:
                handlers.append(logging.FileHandler(f))

        def init_handlers(handler, level, format):
            handler.setLevel(level)
            handler.addFilter(LoggerFilter(level))
            handler.setFormatter(format)

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
            exc_msg = 'Cannot parsing the attribute from [{}] class'.format(dc[idx])
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

        # clear previous attributes
        if self._pre_idx is not None:
            if idx != self._pre_idx:
                for k in self.bucket.param_keys[self._pre_idx]:
                    try:
                        delattr(self, k)
                    except:
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
        base = {1:self._executed, 2:self._reported, 3:self._masked, 4:self._tmp}
        column_index = {1:1, 2:1, 3:0, 4:1}
        for i, dic in base.items():
            if self.bucket.param_keys[i] is not None:
                # if above is None, it's Empty bucket

                if i == 3:
                    filtered = self.bucket(i, copy=True)
                else:
                    if self.msi.path.exists(self.msi.path.join(self.bucket.path, dc[i], self.label)):
                        # check if the current package folder is created
                        filtered = self.bucket(i, self.label, copy=True)
                    else:
                        # if not, the result will be empty, no existing steps in this package
                        filtered = []

                if len(filtered) is 0:
                    dic.clear()
                else:
                    steps = self._sort_params(filtered.df[filtered.df.columns[column_index[i]]])
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
        """internal method to update all subdir information that created in each dataclass folders."""
        msi = self.msi
        steps = [d for d in msi.listdir(self.path) if msi.path.isdir(msi.path.join(self.path, d))]
        self._existing_step_dir = {self._pattern.sub(r'\1', s): s[4:] for s in steps}

        try:
            reports = [d for d in msi.listdir(self.report_path) if msi.path.isdir(msi.path.join(self.report_path, d))]
            self._existing_report_dir = {self._pattern.sub(r'\1', s): s[4:] for s in reports}
        except:
            self._existing_report_dir = dict()

        try:
            masks = [d for d in msi.listdir(self.mask_path) if msi.path.isdir(msi.path.join(self.mask_path, d))]
            self._existing_mask_dir = {self._pattern.sub(r'\1', s): s[4:] for s in masks}
        except:
            self._existing_mask_dir = dict()

        try:
            temps = [d for d in msi.listdir(self.temp_path) if msi.path.isdir(msi.path.join(self.temp_path, d))]
            self._existing_temp_dir = {self._pattern.sub(r'\1', s): s[4:] for s in temps}
        except:
            self._existing_temp_dir = dict()

    def _get_step_dir(self, step_code):
        """return a directory name of working step path"""
        if step_code.upper() in self._existing_step_dir.keys():
            return "{}_{}".format(step_code, self._existing_step_dir[step_code])
        else:
            exc_msg = 'given step code is not exist.'
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

    def _get_temp_dir(self, step_code):
        """return a directory name of working step path"""
        if step_code.upper() in self._existing_temp_dir.keys():
            return "{}_{}".format(step_code, self._existing_temp_dir[step_code])
        else:
            exc_msg = 'given step code is not exist.'
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

    def _get_report_dir(self, step_code):
        """return directory name of result step path"""
        if step_code.upper() in self._existing_report_dir.keys():
            return "{}_{}".format(step_code, self._existing_report_dir[step_code])
        else:
            exc_msg = 'given report code is not exist.'
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

    def _get_mask_dir(self, step_code):
        """return directory name of marking step path"""
        if step_code.upper() in self._existing_mask_dir.keys():
            return "{}_{}".format(step_code, self._existing_mask_dir[step_code])
        else:
            exc_msg = 'given mask code is not exist.'
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

    @staticmethod
    def _sort_params(params):
        """The internal method to remove duplicated parameters
        and return the sorted values"""
        return sorted(list((set(params))))


class InterfaceBase(object):
    """ Base class of InterfaceBuilder.

    Attributes and some function for getting inheritance from the other class
    is defining here in yhr base class

    """
    def __init__(self):
        pass

    def _parse_info_from_processor(self, processor):
        self._procobj = processor
        self._msi = processor.msi
        self._bucket = self._procobj.bucket
        self._label = processor.label
        self._multi_session = self._bucket.is_multi_session()
        self._path = None                   # output path

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

    def _init_attr_for_execution(self):
        # attributes for controlling method execution
        self._step_processed = False  # True if current step listed in procobj._processed_list
        self._order_counter = 0
        self._processed_run_order = []
        self._daemons = dict()
        self._mngs = None

    @property
    def get_daemon(self):
        return self._procobj.get_daemon

    @property
    def msi(self):
        return self._msi

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
            time.sleep(_refresh_rate)
        if message is not None:
            self.logging('debug', '[{}]-#{}-{}'.format(self.step_code,
                                                        str(run_order).zfill(3),
                                                        message), method=method)

    def _report_status(self, run_order):
        self._processed_run_order.append(run_order)
