"""Full suite with automatic connection denial and a swallowed-attempt check."""
if __package__:from scripts import _bootstrap
else:import _bootstrap
import unittest
import sys
from pathlib import Path
from tests import network_guard


if __name__=='__main__':
    sys.path.insert(0,str(Path('tests').resolve())) # existing legacy fixture imports
    suite=unittest.defaultTestLoader.discover('tests',top_level_dir='.')
    result=unittest.TextTestRunner(verbosity=1).run(suite)
    print(f'Accidental external network attempts: {len(network_guard.attempts)}')
    raise SystemExit(0 if result.wasSuccessful() and not network_guard.attempts else 1)
