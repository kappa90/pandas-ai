import pytest
from pandasai.query_builders.base_query_builder import (
    BaseQueryBuilder,
)
from types import (
    SimpleNamespace,
)

try:
    from pandasai.data_loader.semantic_layer_schema import Source
except ImportError:

    class Source:
        pass


class FakeSource:
    """A fake implementation of Source used for testing the check_compatible_sources method.
    The compatibility is determined by the 'compatible' attribute."""

    def __init__(self, compatible: bool):
        self.compatible = compatible

    def is_compatible_source(self, other):
        return self.compatible == getattr(other, "compatible", None)


def test_check_compatible_sources():
    """
    Test the check_compatible_sources static method to ensure that it correctly returns True
    when all provided sources are compatible and False if any of them is not compatible.
    """
    source1 = FakeSource(compatible=True)
    source2 = FakeSource(compatible=True)
    assert BaseQueryBuilder.check_compatible_sources([source1, source2]) is True
    source3 = FakeSource(compatible=False)
    assert BaseQueryBuilder.check_compatible_sources([source1, source3]) is False


def test_build_query_full_schema():
    """
    Test build_query returns a correct SQL query when the schema includes columns,
    GROUP BY, ORDER BY, and LIMIT. This test creates a fake schema with two columns:
    one plain column and one with an expression and alias, along with group_by and order_by clauses.
    """
    col1 = SimpleNamespace(name="col1", expression=None, alias=None)
    col2 = SimpleNamespace(name="col2", expression="SUM(col2)", alias="total")
    schema = SimpleNamespace(
        name="my_table",
        columns=[col1, col2],
        group_by=["col1"],
        order_by=["col1 ASC"],
        limit=100,
    )
    builder = BaseQueryBuilder(schema)
    query = builder.build_query()
    assert "FROM" in query
    assert "GROUP BY" in query
    assert "ORDER BY" in query
    assert "LIMIT" in query
    assert "AS total" in query


def test_get_row_count_minimal_schema():
    """
    Test get_row_count returns a proper SQL query that counts all rows from the table.
    This test creates a minimal schema with an empty columns list and no group_by, order_by, or limit,
    and checks whether the returned query contains 'COUNT(*)', 'FROM', and the table name.
    """
    schema = SimpleNamespace(
        name="test_table", columns=[], group_by=[], order_by=[], limit=None
    )
    builder = BaseQueryBuilder(schema)
    query = builder.get_row_count()
    assert "COUNT(*)" in query
    assert "FROM" in query
    assert "test_table" in query.lower()


def test_get_head_query_with_custom_limit():
    """
    Test get_head_query returns the correct SQL query by ensuring that:
    - When no limit is provided in the schema, it defaults to a LIMIT of 5.
    - When a custom limit is specified via the method parameter, the query reflects that custom limit.
    The test also verifies that the table name is present in the FROM clause.
    """
    schema = SimpleNamespace(
        name="head_table", columns=[], group_by=[], order_by=[], limit=None
    )
    builder = BaseQueryBuilder(schema)
    query_default = builder.get_head_query()
    assert "FROM" in query_default, "The query should contain a FROM clause."
    assert (
        "head_table" in query_default.lower()
    ), "The query should reference the correct table."
    assert "LIMIT" in query_default, "The query should contain a LIMIT clause."
    assert "5" in query_default, "The default limit should be 5."
    query_custom = builder.get_head_query(n=10)
    assert "FROM" in query_custom, "The query should contain a FROM clause."
    assert (
        "head_table" in query_custom.lower()
    ), "The query should reference the correct table."
    assert "LIMIT" in query_custom, "The query should contain a LIMIT clause."
    assert "10" in query_custom, "The custom limit should be reflected in the query."


def test_check_compatible_sources_single_source():
    """
    Test the check_compatible_sources static method with a single source.
    This test ensures that if only one source is provided, the method returns True.
    """
    single_source = FakeSource(compatible=True)
    assert BaseQueryBuilder.check_compatible_sources([single_source]) is True


def test_get_head_query_with_group_by():
    """
    Test get_head_query with a schema that includes a GROUP BY clause.
    This ensures that the GROUP BY is correctly applied to the query, in addition to the
    custom LIMIT and FROM clauses.
    """
    schema = SimpleNamespace(
        name="group_head_table",
        columns=[SimpleNamespace(name="col1", expression=None, alias=None)],
        group_by=["col1"],
        order_by=[],
        limit=None,
    )
    builder = BaseQueryBuilder(schema)
    query = builder.get_head_query(n=3)
    assert (
        "GROUP BY" in query
    ), "The query should include a GROUP BY clause when group_by is provided."
    assert (
        "group_head_table" in query.lower()
    ), "The query should reference the correct table in the FROM clause."
    assert "LIMIT" in query, "The query should contain a LIMIT clause."
    assert "3" in query, "The LIMIT clause should reflect the custom limit value of 3."


def test_build_query_no_columns_no_group_by_order_by_limit():
    """
    Test build_query on a schema with no columns, group_by, order_by, or limit.
    This ensures that the query defaults to SELECT * FROM table, and does not include
    GROUP BY, ORDER BY, or LIMIT clauses in the resulting SQL.
    """
    schema = SimpleNamespace(
        name="minimal_table", columns=[], group_by=[], order_by=[], limit=None
    )
    builder = BaseQueryBuilder(schema)
    query = builder.build_query()
    assert "SELECT" in query, "The query should contain a SELECT clause."
    assert "*" in query, "The query should default to selecting all columns with '*'."
    assert "FROM" in query, "The query should contain a FROM clause."
    assert (
        "minimal_table" in query.lower()
    ), "The query should reference the correct table name."
    assert (
        "GROUP BY" not in query
    ), "The query should not include a GROUP BY clause when none is provided."
    assert (
        "ORDER BY" not in query
    ), "The query should not include an ORDER BY clause when none is provided."
    assert (
        "LIMIT" not in query
    ), "The query should not include a LIMIT clause when none is provided."
