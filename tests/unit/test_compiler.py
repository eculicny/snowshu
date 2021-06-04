import copy
from dataclasses import dataclass
from tests.conftest import RelationTestHelper

import mock
import networkx as nx
import pandas as pd
import pytest
from dfmock import DFMock

import snowshu.core.models.data_types as dt
import snowshu.core.models.materializations as mz
from snowshu.adapters.source_adapters.snowflake_adapter import SnowflakeAdapter
from snowshu.core.compile import RuntimeSourceCompiler
from snowshu.core.models.attribute import Attribute
from snowshu.core.models.relation import Relation
from snowshu.samplings.sample_methods import BernoulliSampleMethod
from snowshu.samplings.samplings import DefaultSampling
from tests.common import query_equalize, rand_string


def stub_out_sampling(rel:Relation)->Relation:
    rel.sampling=DefaultSampling()
    rel.sampling.sample_method=BernoulliSampleMethod(1500,units='rows')
    return rel

def test_analyze_unsampled(stub_relation_set):
    upstream = stub_relation_set.upstream_relation
    upstream.unsampled = True
    dag = nx.DiGraph()
    dag.add_edges_from([(upstream, stub_relation_set.downstream_relation,)])
    compiler = RuntimeSourceCompiler()
    adapter, unsampled_method = [mock.MagicMock() for _ in range(2)]
    adapter.unsampled_statement = unsampled_method

    result = compiler.compile_queries_for_relation(
        upstream, dag, adapter, True)
    assert unsampled_method.called


def test_analyze_iso(stub_relation_set):
    iso = stub_relation_set.iso_relation
    iso=stub_out_sampling(iso)
    dag = nx.DiGraph()
    dag.add_nodes_from([iso])
    compiler = RuntimeSourceCompiler()
    adapter = SnowflakeAdapter()
    result = compiler.compile_queries_for_relation(iso, dag, adapter, True)
    assert query_equalize(iso.compiled_query) == query_equalize(f"""
        WITH
            {iso.scoped_cte('SNOWSHU_COUNT_POPULATION')} AS (
        SELECT
            COUNT(*) AS population_size
        FROM
            {iso.quoted_dot_notation}
        )
        ,{iso.scoped_cte('SNOWSHU_CORE_SAMPLE')} AS (
        SELECT
            *
        FROM 
            {iso.quoted_dot_notation}
            SAMPLE BERNOULLI (1500 ROWS)
        )
        ,{iso.scoped_cte('SNOWSHU_CORE_SAMPLE')}_COUNT AS (
        SELECT
            COUNT(*) AS sample_size
        FROM
            {iso.scoped_cte('SNOWSHU_CORE_SAMPLE')}
        )
        SELECT
            s.sample_size AS sample_size
            ,p.population_size AS population_size
        FROM
            {iso.scoped_cte('SNOWSHU_CORE_SAMPLE')}_COUNT s
        INNER JOIN
            {iso.scoped_cte('SNOWSHU_COUNT_POPULATION')} p
        ON
            1=1
        LIMIT 1
        """)


def test_run_iso(stub_relation_set):
    iso = stub_relation_set.iso_relation
    iso=stub_out_sampling(iso)
    dag = nx.DiGraph()
    dag.add_nodes_from([iso])
    compiler = RuntimeSourceCompiler()
    adapter = SnowflakeAdapter()
    result = compiler.compile_queries_for_relation(iso, dag, adapter, False)
    assert query_equalize(iso.compiled_query) == query_equalize(f"""
        SELECT
            *
        FROM 
            {iso.quoted_dot_notation}
            SAMPLE BERNOULLI (1500 ROWS)
        """)


def test_run_deps_directional(stub_relation_set):
    upstream=stub_relation_set.upstream_relation
    downstream=stub_relation_set.downstream_relation
    for relation in (downstream,upstream,):
        relation.attributes=[Attribute('id',dt.INTEGER)]
        relation=stub_out_sampling(relation)
    upstream.data=pd.DataFrame([dict(id=1),dict(id=2),dict(id=3)])
    dag=nx.DiGraph()
    dag.add_edge(upstream,downstream,direction="directional",remote_attribute='id',local_attribute='id')
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(upstream,dag,adapter,False)
    compiler.compile_queries_for_relation(downstream,dag,adapter,False)
    assert query_equalize(downstream.compiled_query)==query_equalize(f"""
        WITH 
        {downstream.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS ( 
        SELECT 
            * 
        FROM 
        {downstream.quoted_dot_notation}
        WHERE id IN (1,2,3) 
        )
        ,{downstream.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS ( 
        SELECT 
            * 
        FROM 
        {downstream.scoped_cte('SNOWSHU_FINAL_SAMPLE')} SAMPLE BERNOULLI (1500 ROWS) 
        ) 
        SELECT 
            * 
        FROM 
        {downstream.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')}
        """)


def test_run_deps_bidirectional_include_outliers(stub_relation_set):
    upstream=stub_relation_set.upstream_relation
    downstream=stub_relation_set.downstream_relation
    for relation in (downstream,upstream,):
        relation.attributes=[Attribute('id',dt.INTEGER)]
        relation.include_outliers=True    
        relation.max_number_of_outliers=100
        relation=stub_out_sampling(relation)
    upstream.data=pd.DataFrame([dict(id=1),dict(id=2),dict(id=3)])

    dag=nx.DiGraph()
    dag.add_edge(upstream,downstream,direction="bidirectional",remote_attribute='id',local_attribute='id')
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(upstream,dag,adapter,False)
    compiler.compile_queries_for_relation(downstream,dag,adapter,False)
    assert query_equalize(downstream.compiled_query)==query_equalize(f"""
        SELECT 
            * 
        FROM 
        {downstream.quoted_dot_notation}
        WHERE id IN (1,2,3) 
        UNION
        (SELECT
            *
        FROM
        {downstream.quoted_dot_notation}
        WHERE
        id 
        NOT IN 
        (SELECT
            id
        FROM
        {upstream.quoted_dot_notation})
        LIMIT 100) 
        """)

    assert query_equalize(upstream.compiled_query)==query_equalize(f"""
        WITH {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS ( 
        SELECT * FROM 
        {upstream.quoted_dot_notation} 
            WHERE id in (SELECT id 
            FROM 
        {downstream.quoted_dot_notation}) 
        )
        ,{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS ( 
        SELECT 
            * 
        FROM 
            {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} SAMPLE BERNOULLI (1500 ROWS)
        ) 
        SELECT 
            * 
        FROM 
            {relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} 
        UNION 
        (SELECT 
            * 
        FROM 
        {upstream.quoted_dot_notation} 
        WHERE 
            id 
        NOT IN 
            (SELECT 
                id 
            FROM 
        {downstream.quoted_dot_notation}) LIMIT 100)
        """
        )

def test_run_deps_bidirectional_exclude_outliers(stub_relation_set):
    upstream=stub_relation_set.upstream_relation
    downstream=stub_relation_set.downstream_relation
    for relation in (downstream,upstream,):
        relation.attributes=[Attribute('id',dt.INTEGER)]
        relation=stub_out_sampling(relation)
    upstream.data=pd.DataFrame([dict(id=1),dict(id=2),dict(id=3)])

    dag=nx.DiGraph()
    dag.add_edge(upstream,downstream,direction="bidirectional",remote_attribute='id',local_attribute='id')
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(upstream,dag,adapter,False)
    compiler.compile_queries_for_relation(downstream,dag,adapter,False)
    assert query_equalize(downstream.compiled_query)==query_equalize(f"""
        SELECT 
            * 
        FROM 
        {downstream.quoted_dot_notation}
        WHERE id IN (1,2,3) 
        """)

    assert query_equalize(upstream.compiled_query)==query_equalize(f"""
        WITH {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} AS ( 
        SELECT 
            * 
        FROM 
            {upstream.quoted_dot_notation} 
        WHERE 
            id 
        in (SELECT 
                id 
            FROM 
                {downstream.quoted_dot_notation}) ) 
        ,{relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')} AS ( 
            SELECT 
                * 
            FROM 
                {relation.scoped_cte('SNOWSHU_FINAL_SAMPLE')} SAMPLE BERNOULLI (1500 ROWS) 
        ) 
        SELECT 
            * 
        FROM 
        {relation.scoped_cte('SNOWSHU_DIRECTIONAL_SAMPLE')}
        """)


def test_run_deps_directional_line_graph():
    """
        a --dir--> b --dir--> c
    """
    relation_helper = RelationTestHelper()
    relation_a = Relation(name='rel_a', **relation_helper.rand_relation_helper())
    relation_a.attributes = [Attribute('col_a',dt.INTEGER)]
    relation_a.data = pd.DataFrame({"col_a": [1, 2, 3, 4, 5,]})

    relation_b = Relation(name='rel_b', **relation_helper.rand_relation_helper())
    relation_b.attributes = [Attribute('col_b_a',dt.INTEGER), Attribute('col_b_c', dt.VARCHAR)]
    relation_b.data = pd.DataFrame({
        "col_b_a": [1, 3, 4, 4,],
        "col_b_c": ["val1", "val3", "val4", "val4",],
    })

    relation_c = Relation(name='rel_c', **relation_helper.rand_relation_helper())
    relation_c.attributes = [Attribute('col_c',dt.VARCHAR)]

    for relation in (relation_a, relation_b, relation_c,):
        relation=stub_out_sampling(relation)

    dag=nx.DiGraph()
    dag.add_edge(relation_a, relation_b, direction="directional", remote_attribute="col_a", local_attribute="col_b_a")
    dag.add_edge(relation_b, relation_c, direction="directional", remote_attribute="col_b_c", local_attribute="col_c")
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(relation_a, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_b, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_c, dag, adapter, False)

    breakpoint()
    assert False


def test_run_deps_bidirectional_line_graph():
    """
        a --bidir--> b --bidir--> c
    """
    relation_helper = RelationTestHelper()
    relation_a = Relation(name='rel_a', **relation_helper.rand_relation_helper())
    relation_a.attributes = [Attribute('col_a',dt.INTEGER)]
    relation_a.data = pd.DataFrame({"col_a": [1, 2, 3, 4, 5,]})

    relation_b = Relation(name='rel_b', **relation_helper.rand_relation_helper())
    relation_b.attributes = [Attribute('col_b_a',dt.INTEGER), Attribute('col_b_c', dt.VARCHAR)]
    relation_b.data = pd.DataFrame({
        "col_b_a": [1, 3, 4, 4,],
        "col_b_c": ["val1", "val3", "val4", "val4",],
    })

    relation_c = Relation(name='rel_c', **relation_helper.rand_relation_helper())
    relation_c.attributes = [Attribute('col_c',dt.VARCHAR)]

    for relation in (relation_a, relation_b, relation_c,):
        relation=stub_out_sampling(relation)

    dag=nx.DiGraph()
    dag.add_edge(relation_a, relation_b, direction="bidirectional", remote_attribute="col_a", local_attribute="col_b_a")
    dag.add_edge(relation_b, relation_c, direction="bidirectional", remote_attribute="col_b_c", local_attribute="col_c")
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(relation_a, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_b, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_c, dag, adapter, False)

    breakpoint()
    assert False


def test_run_deps_directional_multi_deps():
    """
        a --dir--> c <--dir-- b
    """
    relation_helper = RelationTestHelper()
    relation_a = Relation(name='rel_a', **relation_helper.rand_relation_helper())
    relation_a.attributes = [Attribute('col_a',dt.INTEGER)]
    relation_a.data = pd.DataFrame({"col_a": [1, 2, 3, 4, 5,]})

    relation_b = Relation(name='rel_b', **relation_helper.rand_relation_helper())
    relation_b.attributes = [Attribute('col_b',dt.VARCHAR)]
    relation_b.data = pd.DataFrame({"col_b": ["val1", "val2", "val3", "val4", "val5",],})

    relation_c = Relation(name='rel_c', **relation_helper.rand_relation_helper())
    relation_c.attributes = [Attribute('col_c_a',dt.INTEGER), Attribute('col_c_b', dt.VARCHAR)]

    for relation in (relation_a, relation_b, relation_c,):
        relation=stub_out_sampling(relation)

    dag=nx.DiGraph()
    dag.add_edge(relation_a, relation_c, direction="directional", remote_attribute="col_a", local_attribute="col_c_a")
    dag.add_edge(relation_b, relation_c, direction="directional", remote_attribute="col_b", local_attribute="col_c_b")
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(relation_a, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_b, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_c, dag, adapter, False)

    # TODO add assertions


def test_run_deps_bidirectional_multi_deps():
    """
        a --bidir--> c <--bidir-- b
    """
    relation_helper = RelationTestHelper()
    relation_a = Relation(name='rel_a', **relation_helper.rand_relation_helper())
    relation_a.attributes = [Attribute('col_a',dt.INTEGER)]
    relation_a.data = pd.DataFrame({"col_a": [1, 2, 3, 4, 5,]})

    relation_b = Relation(name='rel_b', **relation_helper.rand_relation_helper())
    relation_b.attributes = [Attribute('col_b',dt.VARCHAR)]
    relation_b.data = pd.DataFrame({"col_b": ["val1", "val2", "val3", "val4", "val5",],})

    relation_c = Relation(name='rel_c', **relation_helper.rand_relation_helper())
    relation_c.attributes = [Attribute('col_c_a',dt.INTEGER), Attribute('col_c_b', dt.VARCHAR)]

    for relation in (relation_a, relation_b, relation_c,):
        relation=stub_out_sampling(relation)

    dag=nx.DiGraph()
    dag.add_edge(relation_a, relation_c, direction="bidirectional", remote_attribute="col_a", local_attribute="col_c_a")
    dag.add_edge(relation_b, relation_c, direction="bidirectional", remote_attribute="col_b", local_attribute="col_c_b")
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(relation_a, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_b, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_c, dag, adapter, False)

    # TODO add assertions


def test_run_deps_mixed_multi_deps():
    r"""
        a --bidir--> c <--dir-- b
         \          / \
          \     bidir  dir
           \       |   |
            \      V   V
             dir-> d   e
    """
    relation_helper = RelationTestHelper()
    relation_a = Relation(name='rel_a', **relation_helper.rand_relation_helper())
    relation_a.attributes = [Attribute('col_a_c',dt.INTEGER), Attribute('col_a_d',dt.VARCHAR)]
    relation_a.data = pd.DataFrame(
        {
            "col_a_c": [1, 2, 3, 4, 5,],
            "col_a_d": ["var_a_1", "var_a_2", "var_a_3", "var_a_1", "var_a_2"],
        }
    )
    relation_b = Relation(name='rel_b', **relation_helper.rand_relation_helper())
    relation_b.attributes = [Attribute('col_b_c',dt.VARCHAR)]
    relation_b.data = pd.DataFrame({"col_b_c": ["val1", "val2", "val3", "val4", "val5",],})

    relation_c = Relation(name='rel_c', **relation_helper.rand_relation_helper())
    relation_c.attributes = [Attribute('col_c_ae',dt.INTEGER), Attribute('col_c_bd', dt.VARCHAR)]
    relation_c.data = pd.DataFrame(
        {
            "col_c_ae": [1, 1, 2, 2, 5, 5, 5,],
            "col_c_bd": ["val1", "val1", "val2", "val2", "val5", "val5", "val5",]
        }
    )

    relation_d = Relation(name='rel_d', **relation_helper.rand_relation_helper())
    relation_d.attributes = [Attribute('col_d_a',dt.INTEGER), Attribute('col_d_c',dt.INTEGER)]

    relation_e = Relation(name='rel_e', **relation_helper.rand_relation_helper())
    relation_e.attributes = [Attribute('col_e_c',dt.INTEGER)]

    for relation in (relation_a, relation_b, relation_c,):
        relation=stub_out_sampling(relation)

    dag=nx.DiGraph()
    dag.add_edge(relation_a, relation_c, direction="bidirectional", remote_attribute="col_a_c", local_attribute="col_c_ae")
    dag.add_edge(relation_a, relation_d, direction="directional", remote_attribute="col_a_d", local_attribute="col_d_a")
    dag.add_edge(relation_b, relation_c, direction="directional", remote_attribute="col_b_c", local_attribute="col_c_bd")
    dag.add_edge(relation_c, relation_d, direction="bidirectional", remote_attribute="col_c_bd", local_attribute="col_d_c")
    dag.add_edge(relation_c, relation_e, direction="directional", remote_attribute="col_c_ae", local_attribute="col_e_c")
    compiler=RuntimeSourceCompiler()
    adapter=SnowflakeAdapter()
    compiler.compile_queries_for_relation(relation_a, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_b, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_c, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_d, dag, adapter, False)
    compiler.compile_queries_for_relation(relation_e, dag, adapter, False)

    # TODO add assertions