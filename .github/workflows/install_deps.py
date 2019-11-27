import os
import subprocess
import sys


EXTRAS = {
    'aws': 'boto',
    'etcd': 'python-etcd',
    'consul': 'python-consul',
    'exhibitor': 'kazoo',
    'zookeeper': 'kazoo',
    'kubernetes': 'kubernetes'
}


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')
    requirements = ['mock', 'flake8', 'pytest', 'pytest-cov'] if what == 'all' else ['behave']
    requirements += ['psycopg2-binary', 'codacy-coverage', 'coverage', 'coveralls', 'setuptools']
    with open('requirements.txt') as f:
        for r in f.read().split('\n'):
            r = r.strip()
            if r == '':
                continue
            if what == 'all' or what not in EXTRAS:
                requirements.append(r)
                continue
            for e, v in EXTRAS.items():
                if r.startswith(v):
                    if e == what:
                        requirements.append(r)
                    break
            else:
                requirements.append(r)
    subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
    r = subprocess.call([sys.executable, '-m', 'pip', 'install'] + requirements)
    s = subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'setuptools'])
    return s | r


if __name__ == '__main__':
    sys.exit(main())
