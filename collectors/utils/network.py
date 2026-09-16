"""Bound RPC and explorer requests in the pinned Brownie process."""
from functools import partialmethod


def configure_timeouts():
    from brownie import web3
    from requests import Session
    web3.provider._request_kwargs['timeout'] = 30
    # Brownie's explorer requests omit timeout; explicit caller timeouts still win.
    Session.request = partialmethod(Session.request, timeout=30)
