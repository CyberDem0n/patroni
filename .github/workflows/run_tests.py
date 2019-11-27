import os
import subprocess
import sys


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')

    if what == 'all':
        flake8 = subprocess.call([sys.executable, 'setup.py', 'flake8'])
        test = subprocess.call([sys.executable, 'setup.py', 'test'])
        return flake8 | test
    env = os.environ.copy()
    env['PATH'] = '/usr/lib/postgresql/10/bin:' + env['PATH']
    env['DCS'] = what
    if subprocess.call(['unbuffer', sys.executable, '-m', 'behave'], env=env) != 0:
        subprocess.call('grep . features/output/*_failed/*postgres?.*', shell=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
