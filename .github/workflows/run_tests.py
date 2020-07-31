import os
import subprocess
import sys
import time


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
    if os.name == 'nt':
        subprocess.call(['pgsql/bin/postgres', '-V'])
        subprocess.call(['pgsql/bin/pg_ctl', 'initdb', '-D', 'fake', '-U', 'postgres'])
        os.environ['COMSPEC'] = 'echo.exe'
        subprocess.call([sys.executable, '-c', "import subprocess; subprocess.call('true', shell=True)"], env=env)
        del os.environ['COMSPEC']
        env['COMSPEC'] = sys.executable + '"' + ' "' + os.path.abspath('.github/workflows/run_behave_windows.py')
        print('"' + env['COMSPEC'] + '"')
        subprocess.call(['pgsql/bin/pg_ctl', '-W', '-D', 'fake', '-l', 'behave.log', 'start'], env=env)
        time.sleep(1)
        with open('behave.log') as f:
            p = 0
            for _ in range(0, 600):
                f.seek(p)
                latest_data = f.read()
                p = f.tell()
                if latest_data:
                    print(latest_data)
                elif os.path.exists('behave.exit'):
                    break
                time.sleep(1)
        with open('behave.exit') as f:
            ret = int(f.read())
    else:
        ret = subprocess.call(unbuffer + [sys.executable, '-m', 'behave'], env=env)

    if ret != 0:
        if subprocess.call('grep . features/output/*_failed/*postgres?.*', shell=True) != 0:
            subprocess.call('grep . features/output/*/*postgres?.*', shell=True)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
