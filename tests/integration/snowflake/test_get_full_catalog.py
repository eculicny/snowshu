import os
from snowshu.core.replica import Replica
from snowshu.utils import PACKAGE_ROOT
from snowshu.core.models.relation import Relation


def test_gets_full_catalog():
    tp = Replica()
    config=os.path.join(PACKAGE_ROOT,"snowshu","templates","replica.yml")
    tp.load_config(config)    
    tp._load_full_catalog()
    
    for relation in tp.full_catalog:
        assert isinstance(relation, Relation)
