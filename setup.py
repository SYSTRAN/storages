from setuptools import setup

setup(
    name='systran.storages',
    version='0.1.0',
    description='systran generic storage implementation',
    author='Jean Senellart',
    author_email='jean.senellart@systrangroup.com',
    url='http://www.systransoft.com',
    scripts=['cli/storages-cli'],
    package_dir={'lib': 'systran'},
    packages=['lib'],
    install_requires=[
        'six',
        'boto',
        'paramiko',
        'requests',
        'requests_mock',
        'scp',
        'openstackclient',
        'jsonschema==2.6.0',
        'packaging>=17.0'
    ]
)
