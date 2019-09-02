from setuptools import setup

setup(
    name='systran.storages',
    version='0.1.0',
    description='systran generic storage implementation',
    author='Jean Senellart',
    author_email='jean.senellart@systrangroup.com',
    url='http://www.systransoft.com',
    scripts=['cli/storages-cli'],
    package_dir={'client': 'cli/storages-cli', 'lib': 'lib'},
    packages=['client', 'lib'],
    install_requires=[
        'six',
        'boto',
        'paramiko',
        'requests',
        'requests_mock',
        'scp',
        'openstackclient',
        'packaging>=17.0'
    ]
)
