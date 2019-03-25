#%% import modules
from .base import config
from .base import dc
from .base import BucketBase
from .base import ProcessorBase
from .base import InterfaceBase
from paralexe import Manager
import os
import time
import re

# import sys
import warnings
import pandas as pd
from ..utils import *

#%% pandas display option setting
pd.set_option("display.max_rows", int(config.get('Display', 'Max_Row')))
pd.set_option("display.max_colwidth", int(config.get('Display', 'Max_Colwidth')))
_refresh_rate = float(config.get('Preferences', 'daemon_refresh_rate'))


#%% The class for handling dataset bucket
class BucketHandler(BucketBase):
    """The class to navigate the dataset information.

    The role of this class is parse the data structure from data folder,
    and keep it as properties to be utilized by other class.

    Notes:
        The Handler level is oriented to filter the data to fine specific group of data.

    Args:
        path (str): project folder.
        client (:obj:'Client', optional): remote client instance.

    Attributes:
        columns:
        path:
        params:
        param_keys:

    Todo:
        Need to update doctrings.
    """
    def __init__(self, path, client=None):
        super(BucketHandler, self).__init__()
        self.__initiate_handler_attributes()
        if client is None:
            self.msi = os
        else:
            # from miresi import SSHInterface
            # self.msi = SSHInterface(client)
            self.msi = client.open_interface()
            self._remote = True
        if path is not None:
            self.set_path(path)
        else:
            pass

    def __initiate_handler_attributes(self):
        self._remote = False
        self._filtered_dataset = {i: None for i in range(len(dc))}
        self._filter_params = {i: None for i in range(len(dc))}
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
        exist_dir = [d for d in self.msi.listdir(self.path) if d in dc]
        if len(exist_dir) != 0:
            missing_dir = list(set(dc).difference(exist_dir))
            if len(missing_dir) != 0:
                for d in missing_dir:
                    self.msi.mkdir(self.msi.path.join(self.path, d))

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
            returncode (int): 1 if dataclass is Empty, else 0.
        """
        def filter_warning(input_items):
            # Print warning message to sys.stderr

            for item in input_items:
                if self.msi.path.exists(self.msi.path.join(self.path, dc[idx], item)):
                    return 0 # to prevent warning when processor initiate with new label
            warnings.warn('Inaccurate filters is used: {}\n'
                          '[{}] class is not filtered.'.format(input_items, dc[idx]))

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
            """The method to perform dataset filtering.
            Args:
                dataset:
                params:
                attributes:
                keyword:
                regex:

            Raises:
                Exception:

            Returns:

            """
            if keyword in params.keys():
                filters = params[keyword]
                if isinstance(filters, str):
                    filters = [filters]
                elif isinstance(filters, list):
                    pass
                else:
                    # Wrong filter
                    raise Exception
                result = []
                if regex is False:
                    if keyword == 'ext':
                        for flt in filters:
                            result.append([finfo for finfo in dataset \
                                           if finfo._asdict()[attributes].endswith(flt)])
                    elif keyword == 'regex':
                        import re
                        for flt in filters:
                            pattern = re.compile(flt)
                            result.append([finfo for finfo in dataset \
                                           for p in [pattern.search(finfo._asdict()[attributes].split('.')[0])] if p])
                    else:
                        for flt in filters:
                            result.append([finfo for finfo in dataset \
                                           if flt in finfo._asdict()[attributes].split('.')[0]])
                else:
                    import re
                    for flt in filters:
                        pattern = re.compile(flt)
                        result.append([finfo for finfo in dataset \
                                       for p in [pattern.search(finfo._asdict()[attributes])] if p])
                return list(set(sum(result, [])))
            else:
                # No matched key
                raise Exception

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


#%% The class for handling Interface
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

    Attributes:


    Todo:
        Need to update doctrings.
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
            Exception:
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
                        raise Exception
                except:
                    self.logging('warn', exc_msg)
                    raise Exception(exc_msg)
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
                        raise Exception
                except:
                    self.logging('warn', exc_msg)
                    raise Exception(exc_msg)
        else:
            exc_msg = 'The given input cannot pass the inspection.'
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

        return input_path

    def _split_step_code(self, step_code):
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
        except:
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
            existing_dir = dict(list(self._existing_step_dir.items()) \
                                + list(self._existing_mask_dir.items()) \
                                + list(self._existing_report_dir.items()))
        else:
            exc_msg = '[{}] is not available mode.'.format(mode)
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

        # inspect idx and subcode
        if idx is not None:
            if isinstance(idx, int) and (idx < 99) and (idx > 0):
                pass
            else:
                exc_msg = 'The given step index is out of range (0 - 99).'
                self.logging('warn', exc_msg)
                raise Exception(exc_msg)
        if subcode is not None:
            if isinstance(subcode, str):
                subcode = subcode.upper()
            if (subcode == 0) or str(subcode) in avail_codes:
                pass
            else:
                exc_msg = 'The given sub-step code is out of range (0 or A-Z).'
                self.logging('warn', exc_msg)
                raise Exception(exc_msg)

        # update suffix to title
        if suffix is not None:
            title = "{}-{}".format(title, suffix)

        # if no folder created so far
        if len(existing_dir.keys()) == 0:
            if subcode is None:
                subcode = 0
            if idx is None:
                # the code for the very first step will be 010
                new_step_code = '01{}'.format(subcode)
            else:
                # if idx are given, then use it
                new_step_code = "{}{}".format(str(idx).zfill(2), subcode)
        else:
            # parse step code from list of executed steps
            existing_codes = sorted(existing_dir.keys())
            existing_titles = [existing_dir[c] for c in existing_codes]

            # check if the same title have been used
            duplicated_title_idx = [i for i, t in enumerate(existing_titles) if title == t]
            if len(duplicated_title_idx) != 0:
                # since will not allow duplicated title, it will be not over 1
                if idx is not None:
                    exc_msg = 'Duplicated title is used, please use suffix to make it distinct.'
                    if idx == int(existing_codes[duplicated_title_idx[0]][:2]):
                        new_step_code = existing_codes[duplicated_title_idx[0]]
                        if subcode is not None:
                            if new_step_code[-1] != str(subcode):
                                self.logging('warn', exc_msg)
                                raise Exception(exc_msg)
                    else:
                        self.logging('warn', exc_msg)
                        raise Exception(exc_msg)
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
                                    raise Exception(exc_msg)
                                new_substep_code = avail_codes[new_substep_code_idx]
                            else:
                                new_substep_code = avail_codes[0]
                else:
                    # idx was not given, generate new one
                    if subcode is not None:
                        # subcode must use with idx
                        exc_msg = 'not allowed to use sub-step code without idx argument.'
                        self.logging('warn', exc_msg)
                        raise Exception(exc_msg)

                    new_substep_code = 0
                    new_step_idx = max(existing_step_idx) + 1

                # compose step code using idx and substep code
                new_step_code = "{}{}".format(str(new_step_idx).zfill(2), new_substep_code)
                if new_step_code in existing_codes:
                    if title not in existing_titles:
                        print(title, existing_titles)
                        exc_msg = 'the step code had been used already.'
                        self.logging('warn', exc_msg)
                        raise Exception(exc_msg)

        # dir name
        new_step_dir = "{}_{}".format(new_step_code, title)

        if mode is 'processing':
            abspath = self.msi.path.join(self.path, new_step_dir)
        elif mode is 'reporting':
            if not self.msi.path.exists(self.report_path):
                self.msi.mkdir(self.report_path)
                self.logging('debug', 'Folder:[{}] is created on [{}] class'.format(self.label, dc[2]))
            abspath = self.msi.path.join(self.report_path, new_step_dir)
        elif mode is 'masking':
            if not self.msi.path.exists(self.mask_path):
                self.msi.mkdir(self.mask_path)
                self.logging('debug', 'Folder:[{}] is created on [{}] class'.format(self.label, dc[2]))
            abspath = self.msi.path.join(self.mask_path, new_step_dir)
        else:
            exc_msg = '[{}] is not available mode.'.format(mode)
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

        if not self.msi.path.exists(abspath):
            self.msi.mkdir(abspath)
            self.logging('debug', '[{}] folder is created.'.format(new_step_dir))
        else:
            self.logging('debug', '[{}] folder is already exist.'.format(new_step_dir))
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
            exc_msg = '[{}] is not available mode.'.format(mode)
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

        if self.msi.path.exists(step_path):
            if len(self.msi.listdir(step_path)) == 0:
                self.msi.rmdir(step_path)
                self.logging('debug', '[{}] folder is deleted.'.format(step_dir))
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

    def destroy_step(self, step_code, mode='processing', verbose=True):
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
            exc_msg = '[{}] is not available mode.'.format(mode)
            self.logging('warn', exc_msg)
            raise Exception(exc_msg)

        if self.msi.path.exists(step_path):
            if len(self.msi.listdir(step_path)) == 0:
                self.msi.rmdir(step_path)
                self.logging('debug', '[{}] folder is deleted.'.format(step_dir))
            else:
                if hasattr(self.msi, 'client'):
                    exc_msg = 'Destroying step dir cannot be performed on remote mode.'.format(step_dir)
                    self.logging('warn', exc_msg)
                    raise Exception
                else:
                    import shutil
                    shutil.rmtree(step_path)
                    self.logging('debug', '[{}] folder contained data, but now it is destroyed.'.format(step_dir))
        self.update()

    def update(self):
        self._parse_existing_subdir()
        self._parse_executed_subdir()

#%% end


class InterfaceHandler(InterfaceBase):
    def __init__(self):
        super(InterfaceHandler, self).__init__()

    def _init_step(self, run_order, mode_idx):
        """hidden method for init_step to run on threading"""
        # check if the init step is not first command user was executed TODO: Freezing issues - need to improve method queue

        if run_order != 0:
            self.logging('warn', 'incorrect order, init_step must be the first method to be executed.',
                         method='init_step')

        if len(self._procobj._waiting_list) is 0:
            # the step_code exists in the processed_list, so no need to wait
            self._step_processed = True
        else:
            loop = True
            while loop is True:
                if self.step_code == self._procobj._waiting_list[0]:
                    loop = False
                time.sleep(_refresh_rate)


        self._procobj.bucket.update()
        if mode_idx is not 2:
            try:
                self._procobj.update_attributes(mode_idx)
            except:
                self._procobj.update_attributes(1)
        self.logging('debug', '[{}]-step initiated.'.format(self.step_code),
                     method='init_step')
        self._report_status(run_order)

    def _set_input(self, run_order, label, input_path, filter_dict,
                   method, idx, mask, join_modifier):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{} label assigned to input_path [{}]'.format(label, input_path), method='set_input')

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
            if filter_dict is None:
                filter_dict = dict()
            else:
                exc_msg = 'insufficient filterdict.'
                if isinstance(filter_dict, dict):
                    for key in filter_dict.keys():
                        if key not in self._bucket._fname_keys:
                            self.logging('warn', exc_msg, method=method_name)
                else:
                    self.logging('warn', exc_msg, method=method_name)

            if self._input_method == 0:
                if idx is None:
                    # point to point input and output match
                    if input_path in self._bucket.params[0].datatypes:
                        dset = self._bucket(0, datatypes=input_path, **filter_dict)
                    else:
                        if mask is True:
                            dset = self._bucket(3, datatypes=input_path, **filter_dict)
                        else:
                            dset = self._bucket(1, pipelines=self._label, steps=input_path, **filter_dict)
                    if len(dset) > 0:
                        if num_inputset == 0:
                            if self._multi_session is True:
                                self._input_ref = {i:(finfo.Subject, finfo.Session) for i, finfo in dset}
                            else:
                                self._input_ref = {i:(finfo.Subject, None) for i, finfo in dset}
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
                                        if len(dset) > 0: # TODO: add logging
                                            self._input_ref[len(self._input_set[label])] = (
                                            dset[idx].Subject, dset[idx].Session)
                                            self._input_set[label].append(dset[idx].Abspath)
                                else:
                                    dset = self._bucket(0, datatypes=input_path,
                                                        subjects=sub, **filter_dict)
                                    if len(dset) > 0:
                                        self._input_ref[len(self._input_set[label])] = (
                                            dset[idx].Subject, None)
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
                                                self._input_set[label].append(dset[idx].Abspath)
                                    else:
                                        dset = self._bucket(3, datatypes=input_path,
                                                            subjects=sub, **filter_dict)
                                        if len(dset) > 0:
                                            self._input_ref[len(self._input_set[label])] = (
                                                dset[idx].Subject, None)
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
                                                self._input_set[label].append(dset[idx].Abspath)
                                    else:
                                        dset = self._bucket(1, pipelines=self._label,
                                                            steps=input_path,
                                                            subjects=sub,
                                                            **filter_dict)
                                        if len(dset) > 0:
                                            self._input_ref[len(self._input_set[label])] = (
                                                dset[idx].Subject, None)
                                            self._input_set[label].append(dset[idx].Abspath)
                    else:
                        self.logging('warn', 'inappropriate index for input data',
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
                if num_inputset == 0:
                    self._input_ref = dict()
                self._input_ref[label] = filter_dict
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
                    else:
                        self.logging('warn', 'inappropriate join_modifier used',
                                     method=method_name)
                self._input_set[label] = spacer.join(list_of_inputs)
            else:
                exc_msg = 'method selection is out of range.'
                self.logging('warn', exc_msg, method=method_name)
            self._report_status(run_order)

    def _set_static_input(self, run_order, label, input_path, filter_dict, idx, mask):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}-{}'.format(label, input_path),
                               method='set_static_input')

            method_name = 'set_static_input'

            if self._main_input is None:
                exc_msg = 'Cannot find input set, run set_input method first.'
                self.logging('warn', exc_msg, method=method_name)
            else:
                self._inspect_label(label, method_name)

            if filter_dict is None:
                filter_dict = dict()
            else:
                exc_msg = 'insufficient filterdict.'
                if isinstance(filter_dict, dict):
                    for key in filter_dict.keys():
                        if key not in self._bucket._fname_keys:
                            self.logging('warn', exc_msg, method=method_name)
                else:
                    self.logging('warn', exc_msg, method=method_name)

            if self._input_method is not 0:
                exc_msg = 'static_input is only allowed to use for input_method=0'
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
                    self._input_set[label].append(dset[idx].Abspath)
            self._report_status(run_order)

    def _set_output(self, run_order, label, modifier, prefix, suffix, ext):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}'.format(label), method='set_output')

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
                    elif isinstance(modifier, str):
                        if self._input_method is not 1:
                            exc_msg = 'Single output name assignment only available for input method=1'
                            self.logging('warn', exc_msg, method=method_name)
                        else:
                            filename = modifier
                    else:
                        exc_msg = 'wrong modifier!'
                        self.logging('warn', exc_msg, method=method_name)
                    fn, fext = split_ext(filename)
                    if prefix is not None:
                        fn = '{}{}'.format(prefix, fn)
                    if suffix is not None:
                        fn = '{}{}'.format(fn, suffix)
                    filename = '.'.join([fn, fext])
                    if ext is not None:
                        if isinstance(ext, str):
                            filename = change_ext(filename, ext)
                        elif ext == False:
                            filename = remove_ext(filename)
                        else:
                            exc_msg = 'wrong extension!'
                            self.logging('warn', exc_msg, method=method_name)
                else:
                    if self._input_method == 1:
                        filename = '{}_output'.format(self.step_code)
                        if prefix is not None:
                            filename = '{}{}'.format(prefix, filename)
                        if suffix is not None:
                            filename = '{}{}'.format(filename, suffix)
                        if ext is not None:
                            filename = '{}.{}'.format(filename, ext)
                    else:
                        fn, fext = split_ext(filename)
                        if prefix is not None:
                            fn = '{}{}'.format(prefix, fn)
                        if suffix is not None:
                            fn = '{}{}'.format(fn, suffix)
                        filename = '.'.join([fn, fext])
                        if ext is not None:
                            if isinstance(ext, str):
                                filename = change_ext(filename, ext)
                            elif ext == False:
                                filename = remove_ext(filename)
                            else:
                                exc_msg = 'wrong extension!'
                                self.logging('warn', exc_msg, method=method_name)
                return filename

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
                exc_msg = '[{}]-unexpected error, might be caused by incorrect input_method.'.format(self.step_code)
                self.logging('warn', exc_msg, method=method_name)

            self._report_status(run_order)

    def _check_output(self, run_order, label, prefix, suffix, ext):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}'.format(label), method='check_output')
            method_name = 'check_output'

            for l, v in self._output_set.items():
                if l == label:
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
                    elif self._input_method == 1: # input_method=1 has only one master output
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
                            exc_msg = '[{}]-unexpected error, might be caused by incorrect input_method.'.format(self.step_code)
                            self.logging('warn', exc_msg, method=method_name)

            if len(self._output_filter) == 0:
                self.logging('warn', '[{}]-insufficient information to generate output_filter.'.format(self.step_code),
                             method='check_output')
            self._report_status(run_order)

    def _set_temporary(self, run_order, label, path_only):
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
                        exc_msg = '[{}]-cannot use temporary step for input_method=1.'.format(self.step_code)
                        self.logging('warn', exc_msg, method=method_name)
                self._inspect_label(label, method_name)

            step_path = self.msi.path.basename(self.path)
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
            self._var_set[label] = value
            self._report_status(run_order)

    def _set_cmd(self, run_order, command):
        """hidden layer to run on daemon"""
        if self._step_processed is True:
            pass
        else:
            self._wait_my_turn(run_order, '{}'.format(command), method='set_cmd')
            self._cmd_set[len(self._cmd_set.keys())] = command
            self._report_status(run_order)

    def _inspect_label(self, label, method_name=None):

        inspect_items = [self._input_set, self._output_set, self._var_set, self._temporary_set]
        for item in inspect_items:
            if label in item.keys():
                exc_msg = '[{}]-The label have been assigned already'.format(self.step_code)
                self.logging('warn', exc_msg, method=method_name)

    def _parse_placeholder(self, manager, command):
        import re
        prefix, surfix = manager.decorator
        raw_prefix = ''.join([r'\{}'.format(chr) for chr in prefix])
        raw_surfix = ''.join([r'\{}'.format(chr) for chr in surfix])

        # The text
        p = re.compile(r"{0}[^{0}{1}]+{1}".format(raw_prefix, raw_surfix))
        return set([obj[len(prefix):-len(surfix)] for obj in p.findall(command)])

    def _inspect_output(self):

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
                             '[{}]-[{}] of existing files detected, now excluding.'.format(self.step_code,
                                                                                           len(index_for_filter)),
                             method='_inspect_output')
                arg_sets = [self._input_set, self._output_set, self._var_set, self._temporary_set]
                for arg_set in arg_sets:
                    for label, value in arg_set.items():
                        if isinstance(value, list):
                            arg_set[label] = [v for i, v in enumerate(value) if i not in index_for_filter]
            else:
                self.logging('debug', '[{}]-all outputs are passed the inspection.'.format(self.step_code),
                             method=self.step_code)
        else:
            self.logging('debug', '[{}]-no output filter'.format(self.step_code),
                         method='_inspect_output')

    def _call_manager(self):
        """call the Manager the command template and its arguments to Manager"""

        managers = []
        if len(self._cmd_set.keys()) == 0:
            self.logging('warn', '[{}]-there is no command'.format(self.step_code),
                         method='_call_manager')
        for i, cmd in sorted(self._cmd_set.items()):
            if hasattr(self.msi, 'client'):
                self.logging('debug', '[{}]-remote client detected.'.format(self.step_code),
                             method='_call_manager')
                mng = Manager(self._procobj.bucket.msi.client)
            else:
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
                                return managers
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
            managers.append(mng)
            self.logging('debug', '[{}]-managers are got all information they need!'.format(self.step_code),
                         method='_call_manager')
        return managers
