from ..errors import *
from ..utils import *
import os
import re
from collections import namedtuple
from ..config import config
import warnings
import pandas as pd

__dc__ = [config.get('Dataset structure', c) for c in ['dataset_path', 'working_path', 'results_path',
                                                       'masking_path', 'temporary_path']]
pd.set_option("display.max_rows", int(config.get('Display', 'Max_Row')))
pd.set_option("display.max_colwidth", int(config.get('Display', 'Max_Colwidth')))


#%% Dataset base class
class BucketBase(object):
    """The class to navigate the dataset information.

    The role of this class is parse the data structure from data folder,
    and keep it as properties to be utilized by other class.

    Notes:
        The Base level is oriented to interact with default properties setting.
        Particularly, scan the folder and parse the data structure.
    """
    def __init__(self):
        self._path = None
        self._dataset = dict()
        self._column_info = dict()
        self._param_keys = dict()
        self._params = dict()
        self._multi_session = False
        self.msi = None

    @property
    def df(self):
        return pd.DataFrame

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

    def compose_columns(self, idx: int, max_depth: int):
        """Method for checking the indexed dataclass has given maximum depth.
        Args:
            idx: index of dataclass
            max_depth: depth of data structure
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
            else:                   # otherwise, response as empty
                return False

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

    def parser(self, path: str) -> (dict, int):
        """Method for checking the maximum depth of the given path,
        and return the file list in the deepest location.
        Args:
            path: the path to be parsed for investigating its data structure
        Returns:
            container: dictionary that storing sub-files and sub-dirs in given path
            max_depth: maximum depth of the given path
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

    @staticmethod
    def _inspect_container(container: dict, max_depth: int) -> (dict, int):
        """Filter out the containers only having empty folder.
        Args:
            container: dictionary that storing sub-files and sub-dirs in given path
            max_depth: maximum depth of the given path
        Returns:
            container: inspected container
            max_depth: maximum depth after inspection
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

    def scan(self, idx: int) -> bool:
        """The method to scan contents in selected dataclass.
        Notes:
            Using 'parser' methods, this method collect the maximal depth
            and collect all file information of the dataclass folder.
            This method is designed to work in both remote and local buckets.
        Args:
            idx: the index of the dataclass.
        Returns:
            True if successful, False otherwise.
        """
        path = self.msi.path.join(self._path, __dc__[idx])
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
            finfo = namedtuple('Finfo', columns)

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
                except TypeError:
                    return False
                except:
                    raise UnexpectedError

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
                    base_path = self.msi.path.join(path, *components)
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
                                if f in [fn.strip() for fn in config['Dataset structure']['ignore'].split(',')]:
                                    pass
                                else:
                                    abspath = self.msi.path.join(base_path, f)
                                    list_finfo.append(finfo(**dict(zip(columns, components + [f, abspath]))))
                        i += 1

            self._dataset[idx] = list_finfo
            self._column_info[idx] = columns
            self._param_keys[idx] = param_keys
            self._params[idx] = param(**param_dict)
        return True

    def update(self, idx: int = None) -> bool or list:
        """The method to scan the dataclass of given index or all in case no index is given.
        Args:
            idx (int): index number for dataclass of interest.
        Returns:
            True if successful, False otherwise.
            Notes:
                In case the idx value is not given, return list containing True or False
                for each dataclass ordered by its index.
        Raises:
            IndexError if idx is out of bound.
        """
        if idx is None:
            resp = list()
            for i in range(len(__dc__)):
                resp.append(self.scan(i))
        elif idx in range(len(__dc__)):
            resp = self.scan(idx)
        # index out of bound
        else:
            raise IndexError
        return resp

    def __call__(self, *args, **kwargs):
        return self

    def __len__(self):
        return 0


class BucketHandler(BucketBase):
    """The class to navigate the dataset information.
    # in python, a handler is most often used to identify functions written to deal with log messages
    # in general, a handler is a term describing any function which takes request and generate a response in some form

    The role of this class is parse the data structure from data folder,
    and keep it as properties to be utilized by other class.
    Notes:
        The Handler level is oriented to filter the data to fine specific group of data.
    Args:
        path (str): project folder.
    """
    def __init__(self, path):
        super(BucketHandler, self).__init__()
        self.__initiate_handler_attributes()
        self.msi = os
        if path is not None:
            self.set_path(path)
        else:
            pass

    def __initiate_handler_attributes(self):
        self._filtered_dataset = {i: None for i in range(len(__dc__))}
        self._filter_params = {i: None for i in range(len(__dc__))}
        self._fname_keys = ['contain', 'ignore', 'ext', 'regex']

    def set_path(self, path):
        """Update the absolute path of the Project folder.

        This method make the dataclass folders if it does not exist.
        Finally this will update the project structure information.

        Args:
            path (str): Project folder
        """
        super(BucketHandler, self).set_path(path)
        self._makedir()
        self.update()

    def _makedir(self):
        """Make dataclass folders if it does not exists"""
        exist_dir = [d for d in self.msi.listdir(self.path) if d in __dc__]
        if len(exist_dir) != 0:
            missing_dir = list(set(__dc__).difference(exist_dir))
            if len(missing_dir) != 0:
                for d in missing_dir:
                    self.msi.mkdir(self.msi.path.join(self.path, d))

    @property
    def fname_keys(self):
        return self._fname_keys

    def _abspath(self, path):
        return self.msi.path.abspath(path)

    def _walk(self, path):
        return self.msi.walk(path)

    def get_df(self, idx, filtered=False):
        """The method to return dataset contents with the pandas DataFrame type.

        Args:
            idx (int): index of dataclass.
            filtered: return filtered dataset structure if True.

        Returns:
            pandas.DataFrame instance that contains data structure of selected dataclass.
        """
        if self._check_empty(idx):
            return None    # empty
        self.apply_filters()
        if filtered is True:
            return pd.DataFrame.from_records(self._filtered_dataset[idx],
                                             columns=self._column_info[idx])
        else:
            return pd.DataFrame.from_records(self._dataset[idx],
                                             columns=self._column_info[idx])

    def _check_empty(self, idx):
        """The method to check if the dataclass is empty or not.

        Args:
            idx (int): index of dataclass

        Returns:
            bool: True if empty, else False
        """
        if self.params[idx] is None:
            return True
        else:
            return False

    def set_filters(self, idx, *args, **kwargs):
        """This method create filtered dataset using given filter.

        Args:
            idx: dataclass index.
            *args: string based filters for legacy method.
            **kwargs: keyword based filters.

        Return:
            return_code (int): 1 if dataclass is Empty, else 0.
        """
        def filter_warning(input_items):
            # Print warning message to sys.stderr

            for item in input_items:
                if self.msi.path.exists(self.msi.path.join(self.path, __dc__[idx], item)):
                    return 0  # to prevent warning when processor initiate with new label
            warnings.warn('Invalid filter: {}\n'
                          '[{}] class is not filtered.'.format(input_items, __dc__[idx]))

        if self._check_empty(idx):
            return 1    # empty
        if self.params[idx] is None:
            self._filter_params[idx] = None
        else:
            keys = self._param_keys[idx]
            avail = sum([self.params[idx]._asdict()[key] for key in keys], [])

            if len(args) != 0:        # Legacy filter method
                if not set(args).issubset(avail):
                    self._filter_params[idx] = None         # reset filters
                    diff_args = list(set(args).difference(avail))
                    filter_warning(diff_args)
                else:
                    self._filter_params[idx] = dict(args=args)

                if kwargs is not None:
                    if not set(kwargs.keys()).issubset(self._fname_keys):
                        self._filter_params[idx] = None     # reset filters
                        diff_kwargs = list(set(kwargs.keys()).difference(self._fname_keys))
                        filter_warning(diff_kwargs)
                    else:
                        for key, value in kwargs.items():
                            self._filter_params[idx][key] = value
            else:                   # New filter method
                if len(kwargs) != 0:
                    filter_keys = sum([keys, self._fname_keys], [])
                    if not set(kwargs.keys()).issubset(filter_keys):
                        self._filter_params[idx] = None     # reset filters
                        diff_kwargs = list(set(kwargs.keys()).difference(filter_keys))
                        filter_warning(diff_kwargs)
                    else:
                        self._filter_params[idx] = dict()
                        for key, value in kwargs.items():
                            self._filter_params[idx][key] = value
                else:
                    self._filter_params[idx] = None         # reset filters
        return 0

    def apply_filters(self):
        """The method to create filtered dataset using stored filter information."""
        def get_filtered_dataset(dataset, params, attributes, keyword, regex=False):
            """The method to perform dataset filtering."""
            if keyword in params.keys():
                filters = params[keyword]
                if isinstance(filters, str):
                    filters = [filters]
                elif isinstance(filters, list):
                    pass
                else:
                    # Wrong filter
                    raise InvalidFilter
                result = []
                if regex is False:
                    if keyword == 'ext':
                        for flt in filters:
                            result.append([finfo for finfo in dataset \
                                           if finfo._asdict()[attributes].endswith(flt)])
                    elif keyword == 'regex':
                        for flt in filters:
                            pattern = re.compile(flt)
                            result.append([finfo for finfo in dataset \
                                           for p in [pattern.search(finfo._asdict()[attributes].split('.')[0])] if p])
                    else:
                        for flt in filters:
                            result.append([finfo for finfo in dataset \
                                           if flt in finfo._asdict()[attributes].split('.')[0]])
                else:
                    for flt in filters:
                        pattern = re.compile(flt)
                        result.append([finfo for finfo in dataset \
                                       for p in [pattern.search(finfo._asdict()[attributes])] if p])
                return list(set(sum(result, [])))
            else:
                # No matched key
                raise KeyError

        for idx, filter_params in self._filter_params.items():
            filtered = self._dataset[idx]
            if filter_params is None:   # No filter
                self._filtered_dataset[idx] = self._dataset[idx]
            else:                       # filter exists
                list_param_keys = list(filter_params.keys())[:]
                finfo_att = self._column_info[idx][:-2]
                fname_att = self._column_info[idx][-2]

                if 'args' in list_param_keys:  # legacy filter method
                    for i, att in enumerate(finfo_att):
                        results = [finfo for finfo in filtered if finfo._asdict()[att] \
                                   in filter_params['args']]
                        if len(results) != 0:
                            filtered = results
                else:               # Updated method, use regex for all filter
                    att_set = dict(zip(self._param_keys[idx], finfo_att))

                    for key, att in att_set.items():
                        if key in list_param_keys:
                            filtered = get_filtered_dataset(filtered, filter_params, att, key, regex=True)
                if 'regex' in list_param_keys:
                    filtered = get_filtered_dataset(filtered, filter_params, fname_att, 'regex')

                if 'contain' in list_param_keys:
                    results = get_filtered_dataset(filtered, filter_params, fname_att, 'contain')
                    if len(results) != 0:
                        filtered = results
                    else:
                        filtered = []

                if 'ignore' in list_param_keys:
                    results = get_filtered_dataset(filtered, filter_params, fname_att, 'ignore')
                    if len(results) != 0:
                        filtered = [finfo for finfo in filtered if finfo not in results]
                    else:
                        filtered = []

                if 'ext' in list_param_keys:
                    filtered = get_filtered_dataset(filtered, filter_params, fname_att, 'ext')
                self._filtered_dataset[idx] = sorted(filtered)


class Bucket(BucketHandler):
    """
    The dataset bucket class is designed to provide low-level convenient function
    for handling and filtering the dataset as object.
    Notes:
        This class contains user friendly print out function for summarizing information for the dataset

    Properties:
        path (str): absolute path of dataset.
        columns (dict): columns for each dataclass
        param_keys (dict): the set of keys to indicate item to filter
        params (dict): the set of parameters which can be used for the filter for each dataclass
    """

    def __init__(self, path):
        super(Bucket, self).__init__(path)
        self._idx = 0

    def __repr__(self):
        if len(self) == 0:
            return '** Empty bucket..'
        else:
            return self.summary

    def __call__(self, idx, *args, **kwargs):
        """Return DataFrame followed applying filters"""
        if 'copy' in kwargs.keys():
            if kwargs['copy'] is True:
                from copy import copy
                _ = kwargs.pop('copy')
                bucket = copy(self)
                bucket._idx = idx
                bucket.set_filters(idx, *args, **kwargs)
                return bucket
            else:
                pass
        else:
            pass
        self._idx = idx
        self.set_filters(idx, *args, **kwargs)
        return self

    def __iter__(self):
        if self._check_empty(self._idx):
            raise Exception('** Empty bucket')
        else:
            for row in self.df.iterrows():
                yield row

    def __getitem__(self, index):
        if self._check_empty(self._idx):
            return None
        else:
            return self.df.loc[index]

    def __len__(self):
        if self._check_empty(self._idx):
            return 0
        else:
            return len(self.df)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def is_multi_session(self):
        return self._multi_session

    def reset(self):
        for k in self._filter_params.keys():
            self._filter_params[k] = None

    @property
    def summary(self):
        return str(self._summary(self._idx))

    @property
    def df(self):
        try:
            return self.get_df(self._idx, filtered=True).sort_values(by=['Abspath']).reset_index(drop=True)
        except (AttributeError, ValueError, TypeError):
            return pd.DataFrame()
        except:
            raise UnexpectedError

    def _summary(self, idx=None):
        if idx is not None:
            self._idx = idx
        summary = ['** Dataset summary\n', 'Path of Dataset: {}'.format(self._path),
                   'Name of Dataset: {}'.format(os.path.basename(self._path)),
                   'Selected DataClass: {}\n'.format(__dc__[self._idx])]
        if self._check_empty(self._idx):
            summary.append('[Empty project]')
        else:
            for key in self._column_info[self._idx][:-2]:
                summary.append('{}(s): {}'.format(key, sorted(list(set(self.df.to_dict()[key].values())))))
            if self._multi_session is True:
                summary.append('Multi session dataset')
            summary.append('')
            if self._filter_params[self._idx] is not None:
                for key, value in self._filter_params[self._idx].items():
                    summary.append('Applied {} filters: {}'.format(key.title(), value))
        return '\n'.join(summary)
