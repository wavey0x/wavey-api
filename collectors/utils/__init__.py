"""Load chain utilities only when requested, keeping storage imports offline."""

from importlib import import_module


def __getattr__(name):
    if name in ('utils', 'Utils'):
        module = import_module('.utils', __name__)
        return module if name == 'utils' else type('Utils', (), {'utils': module})
    raise AttributeError(name)
