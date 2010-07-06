import time
import datetime

class Converter(object):

    @classmethod
    def to_db(cls, obj):
        raise NotImplementedError

    @classmethod
    def from_db(cls, val):
        raise NotImplementedError

class DateTimeConverter(Converter):

    @classmethod
    def to_db(cls, obj):
        return time.mktime(obj.timetuple()) if obj else None

    @classmethod
    def from_db(cls, val):
        return datetime.datetime.fromtimestamp(val) if val else None
