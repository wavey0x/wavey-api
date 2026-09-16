"""Persistent collection scope. Changes require explicit state reconciliation."""

from config import (RESUPPLY_REGISTRY, RESUPPLY_DEPLOYER, INSURANCE_POOL,
                    LOAN_REPAYER, LOAN_CONVERTER, BAD_DEBT_REPAYER, YBS_REGISTRY)

DEPRECATED_YBS_TOKENS = ('0xe3668873D944E4A949DA05fc8bDE419eFF543882',)
HISTORY_STARTS = {'withdrawals': 22830880, 'authorizations': 22034863, 'loans': 22833775}


def feed_config(name):
    if name == 'resupply':
        from scripts.resupply.constants import CONTRACTS
        return dict(chain_id=1, history_version=1, registry=RESUPPLY_REGISTRY,
                    deployer=RESUPPLY_DEPLOYER, insurance_pool=INSURANCE_POOL,
                    loan_repayer=LOAN_REPAYER, loan_converter=LOAN_CONVERTER,
                    bad_debt_repayer=BAD_DEBT_REPAYER, core=CONTRACTS['CORE'],
                    history_starts=HISTORY_STARTS)
    if name == 'ybs':
        return dict(chain_id=1, history_version=1, registry=YBS_REGISTRY,
                    excluded_tokens=list(DEPRECATED_YBS_TOKENS))
    raise ValueError('Unknown feed')
