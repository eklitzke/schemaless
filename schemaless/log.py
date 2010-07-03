import logging


class ClassLogger(object):

    def __get__(self, obj, obj_type=None):
        object_class = obj_type or obj.__class__
        return logging.getLogger(object_class.__module__ + '.' + object_class.__name__)

formatter = logging.Formatter('%(asctime)s :: %(name)s (%(levelname)s) :: %(message)s')
logger = logging.getLogger('schemaless')
