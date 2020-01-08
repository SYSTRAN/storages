import json

import pytest


def pytest_generate_tests(metafunc):
    print(">>>", pytest.config.rootdir)
    with open(str(pytest.config.rootdir / "conftest.json")) as f:
        config = json.load(f)

    if 'storages' in config:
        if 'storage_id' in metafunc.fixturenames:
            metafunc.parametrize("storage_id", config["storages"].keys())

        if 'storages' in metafunc.fixturenames:
            metafunc.parametrize("storages", [config["storages"]])
