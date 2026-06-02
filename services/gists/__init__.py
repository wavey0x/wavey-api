from .db import init_gist_database

__all__ = ["gists_api", "init_gist_database"]


def __getattr__(name):
    if name == "gists_api":
        from .routes import gists_api

        return gists_api
    raise AttributeError(name)
