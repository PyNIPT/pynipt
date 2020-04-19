from pandas import DataFrame
from .config import config


dataclasses = ['dataset_path',
               'working_path',
               'results_path',
               'masking_path',
               'temporary_path']
dc = [config.get('Dataset structure', c) for c in dataclasses]
ignore = config.get('Dataset structure', 'ignore').split(',')
_refresh_rate = float(config.get('Preferences', 'daemon_refresh_rate'))

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
        """This method scans DataStructure of selected dataclass.

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