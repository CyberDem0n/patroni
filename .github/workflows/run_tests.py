import os
import subprocess
import sys


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')

    if what == 'all':
        flake8 = subprocess.call([sys.executable, 'setup.py', 'flake8'])
        test = subprocess.call([sys.executable, 'setup.py', 'test'])
        return flake8 | test
    return subprocess.call([sys.executable, '-m', 'behave'])


if __name__ == '__main__':
    sys.exit(main())
