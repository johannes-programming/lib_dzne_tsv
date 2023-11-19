import lib_dzne_filedata as _fd
import pandas as _pd
from simple_tsv import *


class TSVData(_fd.FileData):
    _ext = '.tsv'
    @classmethod
    def _load(cls, /, file, *, strip=False, **kwargs):
        ans = _tsv.read_DataFrame(file, **kwargs)
        if strip:
            ans = ans.applymap(lambda x: x.strip())
        return cls(ans)
    def _save(self, /, file, *, strip=False):
        ans = self._dataFrame
        if strip:
            ans = ans.applymap(lambda x: x.strip())
        _tsv.write_DataFrame(file, data)
    @staticmethod
    def _default():
        return dict()
    @staticmethod
    def clone_data(data):
        data = _pd.DataFrame(data)
        data = data.copy()
        data = data.applymap(str)
        return data
