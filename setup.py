from setuptools import setup

tests_require = [
    'requests_mock',
    'pytest==4.*',
]

setup(
    name='systran.storages',
    version='0.1.0',
    description='systran generic storage implementation',
    author='Jean Senellart',
    author_email='jean.senellart@systrangroup.com',
    url='http://www.systransoft.com',
    scripts=['cli/storages-cli'],
    package_dir={'systran': 'lib/systran', 'systran.storages': 'lib/systran/storages'},
    packages=['systran', 'systran.storages'],
    tests_require=tests_require,
    extras_require={
        "tests": tests_require,
    },
    install_requires=[
        'six',
        'boto3',
        'paramiko',
        'requests',
        'scp',
        'openstackclient',
    ]
)
