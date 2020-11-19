from setuptools import setup, find_packages

tests_require = [
    'requests_mock',
    'pytest==4.*',
]

setup(
    name='systran-storages',
    version='0.1.0',
    description='systran generic storage implementation',
    author='Jean Senellart',
    author_email='jean.senellart@systrangroup.com',
    url='http://www.systransoft.com',
    packages=find_packages(exclude=["bin"]),
    entry_points={
        "console_scripts": [
            "systran-storages-cli=systran_storages.bin.storages_cli:main",
        ],
    },
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
        'python-swiftclient',
        'python-keystoneclient',
        'requests_toolbelt'
    ]
)
