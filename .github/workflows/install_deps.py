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
        if subprocess.call(['wget', '-qO', '-', 'http://127.0.0.1:8080/'], stdout=devnull, stderr=devnull) == 0:
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
    response = 'HTTP/1.0 200 OK\nContent-Type: application/json\n\n{"servers":["127.0.0.1"],"port":2181}'
    subprocess.Popen("while true; do echo -e '{0}'| nc -l 8181 &> /dev/null; done".format(response), shell=True)
    time.sleep(1)
    subprocess.call(['wget', '-O', '-', 'http://localhost:8181'])
    return 0


def setup_dcs(dcs):
    if dcs == 'kubernetes':
        return setup_kubernetes()
    elif dcs == 'exhibitor':
        return setup_exhibitor()
    return 0


def main():
    what = os.environ.get('DCS', sys.argv[1] if len(sys.argv) > 1 else 'all')
    r = install_requirements(what)
    if what == 'all' or r != 0:
        return
    packages = {
        'etcd': ['etcd'],
        'zookeeper': ['zookeeper', 'zookeeper-bin', 'zookeeperd'],
        'consul': ['consul'],
        'kubernetes': []
    }
    packages['exhibitor'] = packages['zookeeper']
    p = install_packages(packages[what])
    d = setup_dcs(what)
    return r | p | d


if __name__ == '__main__':
    sys.exit(main())
