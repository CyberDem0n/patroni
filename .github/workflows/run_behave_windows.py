import subprocess
import sys


def main():
    try:
        return subprocess.call([sys.executable, '-m', 'behave'])
    except Exception:
        return 1


if __name__ == '__main__':
    ret = main()
    with open('behave.exit', 'w') as f:
        f.write(str(ret))
