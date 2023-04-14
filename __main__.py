import sys
from mdf_reader import read

if __name__ == '__main__':
    kwargs = dict(arg.split('=') for arg in sys.argv[2:])
    if 'sections' in kwargs.keys():
        kwargs.update({ 'sections': [ x.strip() for x in kwargs.get('sections').split(",")] })
    read(sys.argv[1], **kwargs) # kwargs
