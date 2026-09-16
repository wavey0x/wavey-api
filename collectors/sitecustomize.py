"""
Project-level sitecustomize to silence noisy dependency warnings globally.
Imported automatically by Python when present on sys.path.
"""
import warnings

# Suppress pkg_resources deprecation warning emitted by web3
warnings.filterwarnings("ignore", category=UserWarning, module="web3")
