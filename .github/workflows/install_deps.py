import inspect
import os
import shutil
import subprocess
import stat
import sys
import tarfile
import time
import zipfile


def install_requirements(what):
    old_path = sys.path[:]
    w = os.path.join(os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe())))
    sys.path.insert(0, os.path.dirname(os.path.dirname(w)))
    try:
        from setup import EXTRAS_REQUIRE, read
    finally:
        sys.path = old_path
    requirements = ['mock>=2.0.0', 'flake8', 'pytest', 'pytest-cov'] if what == 'all' else ['behave']
    requirements += ['psycopg2-binary', 'coverage']
    for r in read('requirements.txt').split('\n'):
        r = r.strip()
        if r != '':
            extras = {e for e, v in EXTRAS_REQUIRE.items() if v and r.startswith(v[0])}
            if not extras or what == 'all' or what in extras:
                if 'pysyncobj' in r:
                    r = 'git+https://github.com/bakwc/PySyncObj.git@master#egg=pysyncobj'
                    requirements.append('cryptography')
                requirements.append(r)

    subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pip'])
    r = subprocess.call([sys.executable, '-m', 'pip', 'install'] + requirements)
    s = subprocess.call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'setuptools'])
    return s | r


def install_packages(what):
    packages = {
        'zookeeper': ['zookeeper', 'zookeeper-bin', 'zookeeperd'],
        'consul': ['consul'],
    }
    packages['exhibitor'] = packages['zookeeper']
    packages = packages.get(what, [])
    return subprocess.call(['sudo', 'apt-get', 'install', '-y', 'postgresql-13', 'expect-dev'] + packages)


def get_file(url, name):
    from six.moves.urllib.request import urlretrieve
    print('Downloading ' + url)
    urlretrieve(url, name)


def untar(archive, name):
    with tarfile.open(archive) as tar:
        f = tar.extractfile(name)
        dest = os.path.basename(name)
        with open(dest, 'wb') as d:
            shutil.copyfileobj(f, d)
            return dest


def unzip(archive, name):
    with zipfile.ZipFile(archive, 'r') as z:
        name = z.extract(name)
        dest = os.path.basename(name)
        shutil.move(name, dest)
        return dest


def unzip_all(archive):
    print('Extracting ' + archive)
    with zipfile.ZipFile(archive, 'r') as z:
        z.extractall()


def chmod_755(name):
    os.chmod(name, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
             stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)


def unpack(archive, name):
    print('Extracting {0} from {1}'.format(name, archive))
    func = unzip if archive.endswith('.zip') else untar
    name = func(archive, name)
    chmod_755(name)
    return name


def install_etcd():
    version = os.environ.get('ETCDVERSION', '3.3.13')
    platform = {'linux2': 'linux', 'win32': 'windows', 'cygwin': 'windows'}.get(sys.platform, sys.platform)
    dirname = 'etcd-v{0}-{1}-amd64'.format(version, platform)
    ext = 'tar.gz' if platform == 'linux' else 'zip'
    name = '{0}.{1}'.format(dirname, ext)
    url = 'https://github.com/etcd-io/etcd/releases/download/v{0}/{1}'.format(version, name)
    get_file(url, name)
    ext = '.exe' if platform == 'windows' else ''
    return int(unpack(name, '{0}/etcd{1}'.format(dirname, ext)) is None)


def install_postgres():
    version = os.environ.get('PGVERSION', '12.1-1')
    platform = {'darwin': 'osx', 'win32': 'windows-x64', 'cygwin': 'windows-x64'}[sys.platform]
    name = 'postgresql-{0}-{1}-binaries.zip'.format(version, platform)
    get_file('http://get.enterprisedb.com/postgresql/' + name, name)
    unzip_all(name)
    bin_dir = os.path.join('pgsql', 'bin')
    for f in os.listdir(bin_dir):
        chmod_755(os.path.join(bin_dir, f))
    subprocess.call(['pgsql/bin/postgres', '-V'])
    return 0


def setup_kubernetes():
    from six.moves.urllib.request import urlopen

    get_file('https://storage.googleapis.com/minikube/k8sReleases/v1.7.0/localkube-linux-amd64', 'localkube')
    chmod_755('localkube')

    devnull = open(os.devnull, 'w')
    subprocess.Popen(['sudo', 'nohup', './localkube', '--logtostderr=true', '--enable-dns=false'],
                     stdout=devnull, stderr=devnull)

    cert_dir = '/var/lib/localkube/certs'
    ca_crt = 'ca.crt'
    api_crt = 'apiserver.crt'
    api_key = 'apiserver.key'

    for _ in range(0, 120):
        try:
            files = os.listdir(cert_dir)
            if all(f in files for f in (ca_crt, api_crt, api_key)):
                break
        except Exception:
            pass
        time.sleep(1)
    else:
        print('localkube did not start')
        return 1

    subprocess.call('sudo chmod 644 {0}/*'.format(cert_dir), shell=True)

    url = 'https://localhost:8443'
    for _ in range(0, 20):
        try:
            urlopen(url, cafile=(cert_dir + '/' + ca_crt))
        except Exception as e:
            print(str(e))
            if getattr(e, 'code', None) == 401:
                break
        time.sleep(1)
    else:
        print('localkube did not start')
        return 1

    print('Set up .kube/config')
    kube = os.path.join(os.path.expanduser('~'), '.kube')
    os.makedirs(kube)
    with open(os.path.join(kube, 'config'), 'w') as f:
        f.write("""apiVersion: v1
clusters:
- cluster:
    certificate-authority: {0}/{1}
    server: {2}
  name: local
contexts:
- context:
    cluster: local
    user: myself
  name: local
current-context: local
kind: Config
preferences: {{}}
users:
- name: myself
  user:
    client-certificate: {0}/{3}
    client-key: {0}/{4}
""".format(cert_dir, ca_crt, url, api_crt, api_key))
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

    if sys.platform.startswith('linux'):
        r = install_packages(what)
    else:
        r = install_postgres()
    if r != 0:
        return r

    if what.startswith('etcd'):
        return install_etcd()
    elif what == 'kubernetes':
        return setup_kubernetes()
    elif what == 'exhibitor':
        return setup_exhibitor()
    return 0


if __name__ == '__main__':
    sys.exit(main())
