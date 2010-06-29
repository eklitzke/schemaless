import os

GUID_SIZE = 16

def raw_guid(size=GUID_SIZE):
    return os.urandom(size)

def guid(size=GUID_SIZE):
    return raw_guid(size=size).encode('hex')

def to_raw(s):
    return s.decode('hex')

def to_str(r):
    return r.encode('hex')
