import os
import random
import string
import subprocess
import sys


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')

    if what == 'all':
        flake8 = subprocess.call([sys.executable, 'setup.py', 'flake8'])
        test = subprocess.call([sys.executable, 'setup.py', 'test'])
        return flake8 | test
    env = os.environ.copy()
    if sys.platform.startswith('linux'):
        path = '/usr/lib/postgresql/10/bin:.'
        unbuffer = ['timeout', '480', 'unbuffer']
    else:
        path = os.path.abspath(os.path.join('pgsql', 'bin'))
        if sys.platform == 'darwin':
            path += ':.'
        unbuffer = []
    env['PATH'] = path + os.pathsep + env['PATH']
    env['DCS'] = what
    command = unbuffer + [sys.executable, '-m', 'behave']
    if os.name == 'nt':
        symbols = list(string.printable.strip())
        random.shuffle(symbols)
        passwd = ''.join(symbols[:12])
        subprocess.call(['net', 'user', 'postgres', passwd, '/ADD'])
        command = ['PsExec64', '-accepteula', '-user', 'postgres', '-p', passwd, ' '.join(command)]
    if subprocess.call(command, env=env) != 0:
        if subprocess.call('grep . features/output/*_failed/*postgres?.*', shell=True) != 0:
            subprocess.call('grep . features/output/*/*postgres?.*', shell=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
