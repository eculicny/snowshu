import os

import docker
import pytest

from snowshu.configs import PACKAGE_ROOT
from snowshu.core.docker import SnowShuDocker
from snowshu.core.replica.replica_factory import ReplicaFactory


def test_analyze_unsampled(docker_flush):

    replica = ReplicaFactory()

    config = os.path.join(PACKAGE_ROOT, "snowshu", "templates", "replica.yml")
    replica.load_config(config)
    result = replica.analyze(barf=False,compile_method=None).split('\n')
    result.reverse()
    for line in result:
        if "ORDERS" in line:
            assert '\x1b[0;32m100 %\x1b[0m' in line
            break
