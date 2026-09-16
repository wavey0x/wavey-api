"""Run independent scheduled jobs sequentially in the Brownie project."""

from importlib import import_module
import logging


JOBS = (
    'scripts.resupply.main',
    'scripts.ybs_dash.main',
    'scripts.ybs_dash.listeners.event_listener',
)


def main():
    from utils.network import configure_timeouts
    configure_timeouts()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    failures = []
    for module in JOBS:
        try:
            import_module(module).main()
        except Exception:
            logging.exception('Scheduled job failed: %s', module)
            failures.append(module)
    if failures:
        raise RuntimeError('Scheduled jobs failed: ' + ', '.join(failures))
