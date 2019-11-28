import inspect
import os
import subprocess
import stat
import sys
import time


EXTRAS = {
    'aws': 'boto',
    'etcd': 'python-etcd',
    'consul': 'python-consul',
    'exhibitor': 'kazoo',
    'zookeeper': 'kazoo',
    'kubernetes': 'kubernetes'
}


def install_requirements(what):
    old_path = sys.path[:]
    w = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))
    sys.path.insert(0, os.path.dirname(os.path.dirname(w)))
    try:
        from setup import EXTRAS_REQUIRE, read
    finally:
        sys.path = old_path
    requirements = ['mock>=2.0.0', 'flake8', 'pytest', 'pytest-cov'] if what == 'all' else ['behave']
    requirements += ['psycopg2-binary', 'codacy-coverage', 'coverage', 'coveralls', 'setuptools']
    for r in read('requirements.txt').split('\n'):
        r = r.strip()
        if r != '':
            extras = {e for e, v in EXTRAS_REQUIRE.items() if r.startswith(v[0])}
            if not extras or what == 'all' or what in extras:
                requirements.append(r)

    subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
    r = subprocess.call([sys.executable, '-m', 'pip', 'install'] + requirements)
    s = subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'setuptools'])
    return s | r


def install_packages(packages):
    return subprocess.call(['sudo', 'apt-get', 'install', '-y', 'postgresql-10', 'expect-dev', 'wget'] + packages)


def setup_kubernetes():
    w = subprocess.call(['wget', '-qO', 'localkube',
                         'https://storage.googleapis.com/minikube/k8sReleases/v1.7.0/localkube-linux-amd64'])
    if w != 0:
        return w
    os.chmod('localkube', stat.S_IXOTH)
    devnull = open(os.devnull, 'w')
    subprocess.Popen(['sudo', 'nohup', './localkube', '--logtostderr=true', '--enable-dns=false'],
                     stdout=devnull, stderr=devnull)
    for _ in range(0, 120):
        if subprocess.call(['wget', '-qO', '-', 'http://127.0.0.1:8080/']) == 0:
            time.sleep(10)
            break
        time.sleep(1)
    else:
        print('localkube did not start')
        return 1

    subprocess.call('sudo chmod 644 /var/lib/localkube/certs/*', shell=True)
    print('Set up .kube/config')
    kube = os.path.join(os.path.expanduser('~'), '.kube')
    os.makedirs(kube)
    with open(os.path.join(kube, 'config'), 'w') as f:
        f.write("""apiVersion: v1
clusters:
- cluster:
    certificate-authority: /var/lib/localkube/certs/ca.crt
    server: https://127.0.0.1:8443
  name: local
contexts:
- context:
    cluster: local
    user: myself
  name: local
current-context: local
kind: Config
preferences: {}
users:
- name: myself
  user:
    client-certificate: /var/lib/localkube/certs/apiserver.crt
    client-key: /var/lib/localkube/certs/apiserver.key
""")
    return 0


def setup_exhibitor():
    response = '{"servers":["127.0.0.1"],"port":2181}'
    response = 'HTTP/1.0 200 OK\\nContent-Length: {0}\\n\\n{1}'.format(len(response), response)
    s = subprocess.Popen("while true; do echo '{0}'| nc -l 8181 > /dev/null; done".format(response), shell=True)
    return 0 if s.poll() is None else s.returncode


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')
    r = install_requirements(what)
    if what == 'all' or r != 0:
        return r
    packages = {
        'etcd': ['etcd'],
        'zookeeper': ['zookeeper', 'zookeeper-bin', 'zookeeperd'],
        'consul': ['consul'],
    }
    packages['exhibitor'] = packages['zookeeper']
    r = install_packages(packages.get(what, []))
    if r != 0:
        return r
    if what == 'kubernetes':
        return setup_kubernetes()
    elif what == 'exhibitor':
        return setup_exhibitor()
    return 0


if __name__ == '__main__':
    sys.exit(main())
