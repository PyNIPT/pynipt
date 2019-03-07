from ..core.handler import BucketHandler
from ..core.base import dc, os


class Bucket(BucketHandler):
    """
    The dataset bucket class is designed to provide low-level convenient function
    for handling and filtering the dataset as object.

    This class contains user friendly print out function for summarizing information for the dataset
    """

    def __init__(self, path, client=None):
        super(Bucket, self).__init__(path, client)
        self._idx = 0

    def __repr__(self):
        if len(self) == 0:
            return 'Empty bucket..'
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
            raise Exception('Empty bucket')
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

    def is_multi_session(self):
        return self._multi_session

    @property
    def summary(self):
        return str(self._summary(self._idx))

    @property
    def df(self):
        try:
            return self.get_df(self._idx, filtered=True).sort_values(by=['Abspath']).reset_index(drop=True)
        except:
            import pandas as pd
            return pd.DataFrame()

    def _summary(self, idx=None):
        if idx is not None:
            self._idx = idx
        summary = ['** Dataset summary\n', 'Path of Dataset: {}'.format(self._path),
                   'Name of Dataset: {}'.format(os.path.basename(self._path)),
                   'Selected DataClass: {}\n'.format(dc[self._idx])]
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
