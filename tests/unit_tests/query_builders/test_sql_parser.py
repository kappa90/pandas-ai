import pytest
import sqlglot
from pandasai.exceptions import (
    MaliciousQueryError,
)
from pandasai.query_builders.sql_parser import (
    SQLParser,
)


class TestSqlParser:

    @staticmethod
    @pytest.mark.parametrize(
        "query, table_mapping, expected",
        [
            (
                "SELECT * FROM customers",
                {"customers": "clients"},
                'SELECT\n  *\nFROM "clients" AS customers',
            ),
            (
                "SELECT * FROM orders",
                {"orders": "(SELECT * FROM sales)"},
                'SELECT\n  *\nFROM (\n  (\n    SELECT\n      *\n    FROM "sales"\n  )\n) AS orders',
            ),
            (
                "SELECT * FROM customers c",
                {"customers": "clients"},
                'SELECT\n  *\nFROM "clients" AS c',
            ),
            (
                "SELECT c.id, o.amount FROM customers c JOIN orders o ON c.id = o.customer_id",
                {"customers": "clients", "orders": "(SELECT * FROM sales)"},
                'SELECT\n  "c"."id",\n  "o"."amount"\nFROM "clients" AS c\nJOIN (\n  (\n    SELECT\n      *\n    FROM "sales"\n  )\n) AS o\n  ON "c"."id" = "o"."customer_id"',
            ),
            (
                "SELECT d.name AS department, hse.name AS employee, hse.salary\nFROM (\n    SELECT * FROM employees WHERE salary > 50000\n) AS hse\nJOIN departments d ON hse.dept_id = d.id;\n",
                {"employees": "employee", "departments": "department"},
                'SELECT\n  "d"."name" AS "department",\n  "hse"."name" AS "employee",\n  "hse"."salary"\nFROM (\n  SELECT\n    *\n  FROM "employee" AS employees\n  WHERE\n    "salary" > 50000\n) AS "hse"\nJOIN "department" AS d\n  ON "hse"."dept_id" = "d"."id"\n',
            ),
        ],
    )
    def test_replace_table_names(query, table_mapping, expected):
        result = SQLParser.replace_table_and_column_names(query, table_mapping)
        assert result.strip() == expected.strip()

    def test_mysql_transpilation(self):
        query = 'SELECT COUNT(*) AS "total_rows"'
        expected = "SELECT\n  COUNT(*) AS `total_rows`"
        result = SQLParser.transpile_sql_dialect(query, to_dialect="mysql")
        assert result.strip() == expected.strip()

    @staticmethod
    @pytest.mark.parametrize(
        "sql_query, dialect, expected_tables",
        [
            ("SELECT * FROM users;", "postgres", ["users"]),
            (
                "SELECT * FROM users u JOIN orders o ON u.id = o.user_id;",
                "postgres",
                ["users", "orders"],
            ),
            (
                "SELECT * FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;",
                "postgres",
                ["customers", "orders"],
            ),
            (
                "SELECT * FROM (SELECT * FROM employees) AS e;",
                "postgres",
                ["employees"],
            ),
            (
                "\n    WITH sales_data AS (SELECT * FROM sales)\n    SELECT * FROM sales_data;\n    ",
                "postgres",
                ["sales"],
            ),
            ("SELECT u.name FROM users AS u;", "postgres", ["users"]),
            ("SELECT * FROM sales.customers;", "postgres", ["customers"]),
            ('SELECT * FROM "Order Details";', "postgres", ["Order Details"]),
            ("SELECT *", "postgres", []),
        ],
    )
    def test_extract_table_names(sql_query, dialect, expected_tables):
        result = SQLParser.extract_table_names(sql_query, dialect)
        assert SQLParser.extract_table_names(sql_query, dialect) == expected_tables

    def test_replace_table_names_invalid_mapping(self):
        """
        Test handling of an invalid SQL expression in the table mapping.
        This verifies that SQLParser.replace_table_and_column_names raises a ValueError
        when a table mapping value is not a valid SQL expression.
        """
        invalid_mapping = {"customers": "INVALID SQL EXPRESSION"}
        query = "SELECT * FROM customers"
        with pytest.raises(
            ValueError, match="INVALID SQL EXPRESSION is not a valid SQL expression"
        ):
            SQLParser.replace_table_and_column_names(query, invalid_mapping)

    def test_transpile_with_from_dialect(self):
        """
        Test SQL transpilation when using the from_dialect parameter.
        This verifies that the SQL query is correctly parsed using the provided from_dialect
        and then transpiled to the target dialect.
        """
        query = "SELECT 1"
        result = SQLParser.transpile_sql_dialect(
            query, to_dialect="postgres", from_dialect="mysql"
        )
        expected = "SELECT\n  1"
        assert result.strip() == expected.strip()

    def test_replace_table_names_with_unused_mapping(self):
        """
        Test that replace_table_and_column_names does not modify a query
        when the provided table mapping does not match any table in the SQL.
        The resulting SQL remains unchanged except for identifier quoting.
        """
        query = "SELECT * FROM products"
        table_mapping = {"customers": "clients"}
        result = SQLParser.replace_table_and_column_names(query, table_mapping)
        expected = 'SELECT\n  *\nFROM "products"'
        assert result.strip() == expected.strip()
