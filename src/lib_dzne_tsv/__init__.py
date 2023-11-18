import collections as _collections
import contextlib as _contextlib
import csv as _csv

import lib_dzne_filedata as _fd
import pandas as _pd


def reader(target):
    return _csv.reader(target, dialect=Dialect)

def writer(target):
    return _csv.writer(target, dialect=Dialect)




class Dialect(_csv.Dialect):
    delimiter = '\t'
    doublequote = False
    escapechar = None
    lineterminator = '\n'
    quotechar = '"'
    quoting = _csv.QUOTE_NONE
    skipinitialspace = False
    strict = True



class _DictHandler:
    @classmethod
    def from_handler(cls, handler, **kwargs):
        return cls(handler, **kwargs)
    @classmethod
    def from_stream(cls, stream, **kwargs):
        return cls(cls._handler(stream), **kwargs)
    @_contextlib.contextmanager
    def _open_file(*, cls, file, **kwargs):
        with open(file=file, mode=cls._handler.__name__[0]) as stream:
            yield cls.from_stream(stream, **kwargs)
    @classmethod
    def open_file(cls, file, **kwargs):
        return cls._open_file(
            cls=cls,
            file=file,
            **kwargs,
        )
    def __init__(self, handler, *, fieldnames=None):
        cls = type(self)
        if not hasattr(cls, cls._handler_function):
            setattr(cls, cls._handler_function, cls.handle)
        if not hasattr(cls, cls._handler.__name__):
            setattr(cls, cls._handler.__name__, property(cls._get_handler))
            #setattr(cls, cls._handler.__name__, property(cls._get_handler))
        self.line_num = 0
        self.handler = handler
        self.fieldnames = fieldnames
    @property
    def handler(self):
        return self._handler
    @handler.setter
    def handler(self, value):
        cls = type(self)
        if not hasattr(value, cls._handler_function):
            value = cls._handler(value)
        self._handler = value
    def _get_handler(self):
        return self.handler
    def _set_handler(self, value):
        self.handler = value
    def _set_fieldnames(self, value):
        if value is None:
            pass
        elif type(value) is int:
            if value < 0:
                raise ValueError("The property fieldnames cannot be a negative integer! ")
        else:
            value = tuple(value)
            errors = list()
            if hasattr(self, '_fieldnames'):
                if type(self._fieldnames) is int:
                    if len(value) != self._fieldnames:
                        errors.append(
                            ValueError(
                                "The header has not the preset width! "
                            )
                        )
            for item, count in _collections.Counter(value).items():
                if count > 1:
                    errors.append(
                        KeyError(
                            f"The fieldname {item} occures more than once! "
                        )
                    )
            if len(errors):
                raise ExceptionGroup("Improperly formatted! ", errors)
        self._fieldnames = value
    def use_handler(self, *args, **kwargs):
        cls = type(self)
        func = getattr(self.handler, cls._handler_function)
        return func(*args, **kwargs)
    def handle(self, *args, **kwargs):
        ans = self._handle(*args, **kwargs)
        self.line_num += 1
        return ans




class DictReader(_DictHandler):
    _handler = reader
    _handler_function = '__next__'
    def __iter__(self):
        return self
    @property
    def fieldnames(self):
        if (self._fieldnames is None) or (type(self._fieldnames) is int):
            self.fieldnames = self.use_handler()
        return self._fieldnames
    @fieldnames.setter
    def fieldnames(self, value):
        self._set_fieldnames(value)
    def _handle(self):
        fieldnames = self.fieldnames
        line = self.use_handler()
        if len(fieldnames) != len(line):
            raise ValueError(f"Row {self.line_num} has wrong width! ")
        return dict(zip(fieldnames, line))




class DictWriter(_DictHandler):
    _handler = writer
    _handler_function = 'writerow'
    @property
    def fieldnames(self):
        return self._fieldnames
    @fieldnames.setter
    def fieldnames(self, value):
        self._set_fieldnames(value)
        if type(self._fieldnames) is tuple:
            self.use_handler(self._fieldnames)
    def _handle(self, row):
        if (self.fieldnames is None) or (type(self.fieldnames) is int):
            self.fieldnames = row.keys()
        else:
            errors = list()
            for x in row.keys():
                if x not in self.fieldnames:
                    errors.append(
                        KeyError(
                            f"The key {x} is missing! "
                        )
                    )
            for x in self.fieldnames:
                if x not in row.keys():
                    errors.append(
                        KeyError(
                            f"The key {x} is unexpected! "
                        )
                    )
            if len(errors):
                raise ExceptionGroup("Improper row! ", errors)
        values = [row[x] for x in self.fieldnames]
        self.use_handler(values)


class TSVData(_fd.FileData):
    _ext = '.tsv'
    @classmethod
    def _load(cls, /, file, *, strip=False, **kwargs):
        with DictReader.open_file(file, **kwargs) as dictReader:
            ans = _pd.DataFrame(dictReader)
        if strip:
            ans = ans.applymap(lambda x: x.strip())
        return cls(ans)
    def _save(self, /, file, *, strip=False):
        df = self.data
        if strip:
            df = df.applymap(lambda x: x.strip())
        with DictWriter.open_file(file) as dictWriter:
            dictWriter.fieldnames = df.columns
            for i, row in df.iterrows():
                dictWriter.writerow(row)
    @staticmethod
    def _default():
        return dict()
    @staticmethod
    def clone_data(data):
        data = _pd.DataFrame(data)
        data = data.copy()
        data = data.applymap(str)
        return data
