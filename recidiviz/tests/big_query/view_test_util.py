# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Utility class for testing BQ views against Postgres"""
import datetime
import logging
import re
import unittest
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Type

import attr
import pandas as pd
import pytest
import sqlalchemy
from google.cloud import bigquery
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm.session import close_all_sessions
from sqlalchemy.sql import sqltypes

from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
    BigQueryView,
    BigQueryViewBuilder,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
    augment_raw_data_df_with_metadata_columns,
)
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    UPDATE_DATETIME_PARAM_NAME,
    DirectIngestPreProcessedIngestView,
    DirectIngestPreProcessedIngestViewBuilder,
    RawTableViewType,
)
from recidiviz.persistence.database.session import Session
from recidiviz.tools.postgres import local_postgres_helpers


def _replace_iter(query: str, regex: str, replacement: str, flags: int = 0) -> str:
    compiled = re.compile(regex, flags)
    for match in re.finditer(compiled, query):
        query = query.replace(match[0], replacement.format(**match.groupdict()))
    return query


DEBUG = True


@attr.s(frozen=True)
class MockTableSchema:
    """Defines the table schema to be used when mocking this table in Postgres"""

    data_types: Dict[str, sqltypes.SchemaType] = attr.ib()

    @classmethod
    def from_sqlalchemy_table(cls, table: sqlalchemy.Table) -> "MockTableSchema":
        data_types = {}
        for column in table.columns:
            if isinstance(column.type, sqltypes.Enum):
                data_types[column.name] = sqltypes.String(255)
            else:
                data_types[column.name] = column.type
        return cls(data_types)

    @classmethod
    def from_raw_file_config(
        cls, config: DirectIngestRawFileConfig
    ) -> "MockTableSchema":
        return cls(
            {
                # Postgres does case-sensitive lowercase search on all non-quoted
                # column (and table) names. We lowercase all the column names so that
                # a query like "SELECT MyCol FROM table;" finds the column "mycol".
                column.name.lower(): sqltypes.String
                for column in config.columns
            }
        )

    @classmethod
    def from_big_query_schema_fields(
        cls, bq_schema: List[bigquery.SchemaField]
    ) -> "MockTableSchema":
        data_types = {}
        for field in bq_schema:
            field_type = bigquery.enums.SqlTypeNames(field.field_type)
            if field_type is bigquery.enums.SqlTypeNames.STRING:
                data_type = sqltypes.String(255)
            elif field_type is bigquery.enums.SqlTypeNames.INTEGER:
                data_type = sqltypes.Integer
            elif field_type is bigquery.enums.SqlTypeNames.FLOAT:
                data_type = sqltypes.Float
            elif field_type is bigquery.enums.SqlTypeNames.DATE:
                data_type = sqltypes.Date
            elif field_type is bigquery.enums.SqlTypeNames.BOOLEAN:
                data_type = sqltypes.Boolean
            else:
                raise ValueError(
                    f"Unhandled big query field type '{field_type}' for attribute '{field.name}'"
                )
            data_types[field.name] = data_type
        return cls(data_types)


_INITIAL_SUFFIX_ASCII = ord("a")


class NameGenerator(Iterator[str]):
    def __init__(self, prefix: str = "") -> None:
        self.prefix = prefix
        self.counter = 0

    def _get_name(self, counter: int) -> str:
        return self.prefix + chr(_INITIAL_SUFFIX_ASCII + counter)

    def __next__(self) -> str:
        type_name = self._get_name(self.counter)
        self.counter += 1
        return type_name

    def __iter__(self) -> Iterator[str]:
        return self

    def all_names_generated(self) -> List[str]:
        return [self._get_name(i) for i in range(self.counter)]


_CREATE_ARRAY_CONCAT_AGG_FUNC = """
CREATE AGGREGATE array_concat_agg (anyarray)
(
    sfunc = array_cat,
    stype = anyarray,
    initcond = '{}'
);
"""

_DROP_ARRAY_CONCAT_AGG_FUNC = """
DROP AGGREGATE array_concat_agg (anyarray)
"""

_CREATE_COND_FUNC = """
CREATE OR REPLACE FUNCTION cond(boolean, anyelement, anyelement)
 RETURNS anyelement LANGUAGE SQL AS
$func$
SELECT CASE WHEN $1 THEN $2 ELSE $3 END
$func$;
"""

_DROP_COND_FUNC = """
DROP FUNCTION cond(boolean, anyelement, anyelement)
"""

MAX_POSTGRES_TABLE_NAME_LENGTH = 63

DEFAULT_FILE_UPDATE_DATETIME = datetime.datetime(2021, 4, 14, 0, 0, 0)
DEFAULT_QUERY_RUN_DATETIME = datetime.datetime(2021, 4, 15, 0, 0, 0)


@pytest.mark.uses_db
class BaseViewTest(unittest.TestCase):
    """This is a utility class that allows BQ views to be tested using Postgres instead.

    This is NOT fully featured and has some shortcomings, most notably:
    1. It uses naive regexes to rewrite parts of the query. This works for the most part but may produce invalid
       queries in some cases. For instance, the lazy capture groups may capture the wrong tokens in nested function
       calls.
    2. Postgres can only use ORDINALS when unnesting and indexing into arrays, while BigQuery uses OFFSETS (or both).
       This does not translate the results (add or subtract one). So long as the query consistently uses one or the
       other, it should produce correct results.
    3. This does not (yet) support chaining of views. To test a view query, any tables or views that it queries from
       must be created and seeded with data using `create_table`.
    4. Not all BigQuery SQL syntax has been translated, and it is possible that some features may not have equivalent
       functionality in Postgres and therefore can't be translated.

    Given these, it may not make sense to use this for all of our views. If it prevents you from using BQ features that
    would be helpful, or creates more headaches than value it provides, it may not be necessary.
    """

    # Stores the on-disk location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    postgres_engine: Optional[Engine]

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        # Stores the list of mock tables that have been created as (dataset_id, table_id) tuples.
        self.mock_bq_to_postgres_tables: Dict[BigQueryAddress, str] = {}

        self.type_name_generator = NameGenerator("__type_")
        self.postgres_engine = create_engine(
            local_postgres_helpers.on_disk_postgres_db_url()
        )

        # Implement ARRAY_CONCAT_AGG function that behaves the same (ARRAY_AGG fails on empty arrays)
        self._execute_statement(_CREATE_ARRAY_CONCAT_AGG_FUNC)

        # Implement COND to behave like IF, as munging to case is error prone
        self._execute_statement(_CREATE_COND_FUNC)

    def _execute_statement(self, statement: str) -> None:
        session = Session(bind=self.postgres_engine)
        try:
            session.execute(statement)
            session.commit()
        except Exception as e:
            logging.warning("Failed to execute statement: %s\n%s", e, statement)
            session.rollback()
        finally:
            session.close()

    def tearDown(self) -> None:
        close_all_sessions()

        if self.mock_bq_to_postgres_tables:
            # Execute each statement one at a time for resilience.
            for postgres_table_name in self.mock_bq_to_postgres_tables.values():
                self._execute_statement(f"DROP TABLE IF EXISTS {postgres_table_name}")
            for type_name in self.type_name_generator.all_names_generated():
                self._execute_statement(f"DROP TYPE {type_name}")
        self._execute_statement(_DROP_ARRAY_CONCAT_AGG_FUNC)
        self._execute_statement(_DROP_COND_FUNC)

        if self.postgres_engine is not None:
            self.postgres_engine.dispose()
            self.postgres_engine = None

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def create_mock_bq_table(
        self,
        dataset_id: str,
        table_id: str,
        mock_schema: MockTableSchema,
        mock_data: pd.DataFrame,
    ) -> None:
        postgres_table_name = self.register_bq_address(
            address=BigQueryAddress(dataset_id=dataset_id, table_id=table_id)
        )
        mock_data.to_sql(
            name=postgres_table_name,
            con=self.postgres_engine,
            dtype=mock_schema.data_types,
            index=False,
        )

    def create_mock_raw_file(
        self,
        region_code: str,
        file_config: DirectIngestRawFileConfig,
        mock_data: List[Tuple[Any, ...]],
        update_datetime: datetime.datetime = DEFAULT_FILE_UPDATE_DATETIME,
    ) -> None:
        mock_schema = MockTableSchema.from_raw_file_config(file_config)

        raw_data_df = augment_raw_data_df_with_metadata_columns(
            raw_data_df=pd.DataFrame(mock_data, columns=mock_schema.data_types.keys()),
            file_id=0,
            utc_upload_datetime=update_datetime,
        )

        # For the raw data tables we make the table name `us_xx_file_tag`. It would be
        # closer to the actual produced query to make it something like
        # `us_xx_raw_data_file_tag`, but that more easily gets us closer to the 63
        # character hard limit imposed by Postgres.
        self.create_mock_bq_table(
            dataset_id=region_code.lower(),
            # Postgres does case-sensitive lowercase search on all non-quoted
            # table (and column) names. We lowercase all the table names so that
            # a query like "SELECT my_col FROM MyTable;" finds the table "mytable".
            table_id=file_config.file_tag.lower(),
            mock_schema=mock_schema,
            mock_data=raw_data_df,
        )

    def query_raw_data_view_for_builder(
        self,
        view_builder: DirectIngestPreProcessedIngestViewBuilder,
        dimensions: List[str],
        query_run_dt: datetime.datetime = DEFAULT_QUERY_RUN_DATETIME,
    ) -> pd.DataFrame:
        view: BigQueryView = view_builder.build()
        view_query = view.expanded_view_query(
            DirectIngestPreProcessedIngestView.QueryStructureConfig(
                raw_table_view_type=RawTableViewType.PARAMETERIZED
            )
        )
        view_query = view_query.replace(
            f"@{UPDATE_DATETIME_PARAM_NAME}",
            f"TIMESTAMP '{query_run_dt.isoformat()}'",
        )

        return self.query_view(
            view.table_for_query, view_query, data_types={}, dimensions=dimensions
        )

    def query_view_for_builder(
        self,
        view_builder: BigQueryViewBuilder,
        data_types: Dict[str, Type],
        dimensions: List[str],
    ) -> pd.DataFrame:
        if isinstance(view_builder, DirectIngestPreProcessedIngestViewBuilder):
            raise ValueError(
                f"Found view builder type [{type(view_builder)}] - use "
                f"query_raw_data_view_for_builder() for this type instead."
            )

        view: BigQueryView = view_builder.build()
        return self.query_view(
            view.table_for_query, view.view_query, data_types, dimensions
        )

    def query_view(
        self,
        table_address: BigQueryAddress,
        view_query: str,
        data_types: Dict[str, Type],
        dimensions: List[str],
    ) -> pd.DataFrame:
        view_query = self._rewrite_sql(view_query)
        # TODO(#5533): Instead of using read_sql_query, we can use
        # `create_view` and `read_sql_table`. That can take a schema which will
        # solve some of the issues. As part of adding `dimensions` to builders
        # (below) we should likely just define a full output schema.
        results = pd.read_sql_query(view_query, con=self.postgres_engine)
        results = results.astype(data_types)
        # TODO(#5533): If we add `dimensions` to all `BigQueryViewBuilder`, instead of just
        # `MetricBigQueryViewBuilder`, then we can reuse that here instead of forcing the caller to specify them
        # manually.
        results = results.set_index(dimensions)
        results = results.sort_index()

        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        logging.debug("Results for `%s`:\n%s", table_address, results.to_string())

        return results

    def query_view_chain(
        self,
        view_builders: Sequence[BigQueryViewBuilder],
        data_types: Dict[str, Type],
        dimensions: List[str],
    ) -> pd.DataFrame:
        for view_builder in view_builders[:-1]:
            self.create_view(view_builder)
        return self.query_view_for_builder(view_builders[-1], data_types, dimensions)

    def create_view(self, view_builder: BigQueryViewBuilder) -> None:
        view: BigQueryView = view_builder.build()
        table_location = view.table_for_query
        self.register_bq_address(table_location)
        query = (
            f"CREATE TABLE "
            f"`{view.project}.{table_location.dataset_id}.{table_location.table_id}` "
            f"AS ({view.view_query})"
        )
        query = self._rewrite_sql(query)
        self._execute_statement(query)

        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        results = pd.read_sql_query(
            f"SELECT * FROM {self.mock_bq_to_postgres_tables[table_location]}",
            con=self.postgres_engine,
        )
        logging.debug("Results for `%s`:\n%s", table_location, results.to_string())

    def register_bq_address(self, address: BigQueryAddress) -> str:
        """Registers a BigQueryAddress in the map of address -> Postgres tables. Returns
        the corresponding Postgres table name.
        """
        # Postgres does not support '.' in table names, so we instead join them with an underscore.
        postgres_table_name = "_".join([address.dataset_id, address.table_id])
        if len(postgres_table_name) > MAX_POSTGRES_TABLE_NAME_LENGTH:
            new_postgres_table_name = postgres_table_name[
                :MAX_POSTGRES_TABLE_NAME_LENGTH
            ]

            for (
                other_address,
                other_postgres_table_name,
            ) in self.mock_bq_to_postgres_tables.items():
                if (
                    other_postgres_table_name == new_postgres_table_name
                    and address != other_address
                ):
                    raise ValueError(
                        f"Truncated postgres table name [{new_postgres_table_name}] "
                        f"for address [{address}] collides with name for "
                        f"[{other_address}]."
                    )

            logging.warning(
                "Table name [%s] too long, truncating to [%s]",
                postgres_table_name,
                new_postgres_table_name,
            )
            postgres_table_name = new_postgres_table_name

        if address in self.mock_bq_to_postgres_tables:
            if self.mock_bq_to_postgres_tables[address] != postgres_table_name:
                raise ValueError(
                    f"Address [{address}] already has a different postgres table associated with it: {self.mock_bq_to_postgres_tables[address]}"
                )

        self.mock_bq_to_postgres_tables[address] = postgres_table_name
        return postgres_table_name

    def _rewrite_sql(self, query: str) -> str:
        """Modifies the SQL query, translating BQ syntax to Postgres syntax where necessary."""
        query = self._rewrite_table_references(query)

        query = self._rewrite_unnest_with_offset(query)

        # Replace '#' comments with '--'
        query = _replace_iter(query, r"#", "--")

        # Update % signs in format args to be double escaped
        query = _replace_iter(
            query, r"(?P<first_char>[^\%])\%(?P<fmt>[A-Za-z])", "{first_char}%%{fmt}"
        )

        # Must index the array directly, instead of using OFFSET or ORDINAL
        query = _replace_iter(query, r"\[OFFSET\((?P<offset>.+?)\)\]", "[{offset}]")
        query = _replace_iter(query, r"\[ORDINAL\((?P<ordinal>.+?)\)\]", "[{ordinal}]")

        # Array concatenation is performed with the || operator
        query = _replace_iter(query, r"ARRAY_CONCAT\((?P<first>[^,]+?)\)", "({first})")
        query = _replace_iter(
            query,
            r"ARRAY_CONCAT\((?P<first>[^,]+?), (?P<second>[^,]+?)\)",
            "({first} || {second})",
        )

        # Postgres requires you to specify the dimension of the array to measure the length of. BigQuery doesn't
        # support multi-dimensional arrays so mapping to cardinality, which returns the total number of elements in an
        # array, provides the same behavior. Simply specifying 1 as the dimension to measure will differ in behavior
        # for empty arrays.
        query = _replace_iter(query, r"ARRAY_LENGTH", "CARDINALITY")

        # IN UNNEST doesn't work in postgres when passing an array column, instead use = ANY
        query = _replace_iter(query, r"IN UNNEST", "= ANY")

        # ENDS_WITH doesn't exist in postgres so use LIKE instead
        query = _replace_iter(
            query,
            r"ENDS_WITH\((?P<column>.+?), \'(?P<predicate>.+?)\'\)",
            "{column} LIKE '%%{predicate}'",
        )

        # Postgres doesn't have ANY_VALUE, but since we don't care what the value is we can just use MIN
        query = _replace_iter(query, r"ANY_VALUE\((?P<column>.+?)\)", "MIN({column})")

        # The interval must be quoted.
        query = _replace_iter(
            query,
            r"INTERVAL (?P<num>\d+?) (?P<unit>\w+?)(?P<end>\W)",
            "INTERVAL '{num} {unit}'{end}",
        )
        query = _replace_iter(
            query, r"(^| )DATE\s*\(", " make_date(", flags=re.IGNORECASE
        )

        # Postgres doesn't have DATE_DIFF where you can specify the units to return, but subtracting two dates always
        # returns the number of days between them.
        query = _replace_iter(
            query,
            r"DATE_DIFF\((?P<first>.+?), (?P<second>.+?), DAY\)",
            "({first} - {second})",
        )

        # Date arithmetic just uses operators (e.g. -), not function calls
        query = _replace_iter(
            query,
            r"DATE_SUB\((?P<first>.+?), INTERVAL '(?P<second>.+?)'\)",
            "({first} - INTERVAL '{second}')::date",
        )
        query = _replace_iter(
            query,
            r"DATE_ADD\((?P<first>.+?), INTERVAL '(?P<second>.+?)'\)",
            "({first} + INTERVAL '{second}')::date",
        )

        # The parameters for DATE_TRUNC are in the opposite order, and the interval must be quoted.
        query = _replace_iter(
            query,
            r"DATE_TRUNC\((?P<first>.+?), (?P<second>.+?)\)",
            "DATE_TRUNC('{second}', {first})::date",
        )

        # The REGEXP_CONTAINS function does not exist in postgres, so we replace with
        # 'SUBSTRING() IS NOT NULL', which has the same behavior.
        query = _replace_iter(
            query,
            r"REGEXP_CONTAINS\((?P<first>.+?), (?P<second>.+?)\)",
            "SUBSTRING({first}, {second}) IS NOT NULL",
        )

        # EXTRACT returns a double in postgres, but for all part types shared between
        # the two, bigquery returns an int
        query = _replace_iter(
            query,
            r"EXTRACT\((?P<clause>.+)\)(?P<end>[^:])",
            "EXTRACT({clause})::integer{end}",
        )

        # LAST_DAY doesn't exist in postgres, so replace with the logic to calculate it
        query = _replace_iter(
            query,
            r"LAST_DAY\((?P<column>.+?)\)",
            "DATE_TRUNC('MONTH', {column} + INTERVAL '1 MONTH')::date - 1",
        )

        # Postgres doesn't have SAFE_DIVIDE, instead we use NULLIF to make the denominator NULL if it was going to be
        # zero, which will make the whole expression NULL, the same behavior as SAFE_DIVIDE.
        query = _replace_iter(
            query,
            r"SAFE_DIVIDE\((?P<first>.+?), (?P<second>.+?)\)",
            "({first} / NULLIF({second}, 0))",
        )

        query = _replace_iter(query, r"SAFE_CAST", "CAST", flags=re.IGNORECASE)

        # Date/time parsing functions are different in Postgres
        query = _replace_iter(
            query,
            r"(SAFE\.)?PARSE_TIMESTAMP\((?P<fmt>.+?), (?P<col>.+?)\)",
            "TO_TIMESTAMP({col}, {fmt})",
            flags=re.IGNORECASE,
        )

        query = _replace_iter(
            query,
            r"(SAFE\.)?PARSE_DATE\((?P<fmt>.+?), (?P<col>.+?)\)",
            "TO_DATE({col}, {fmt})",
            flags=re.IGNORECASE,
        )

        # String type does not exist in Postgres
        query = _replace_iter(
            query,
            r"CAST\((?P<value>.+?) AS STRING\)",
            "CAST({value} AS VARCHAR)",
            flags=re.IGNORECASE,
        )
        query = _replace_iter(query, r"IFNULL", "COALESCE", flags=re.IGNORECASE)
        query = _replace_iter(query, r"(^| )IF\s*\(", " COND(", flags=re.IGNORECASE)

        # Replace DATETIME type with TIMESTAMP, attempting to not pick up the term
        # 'datetime' when used in a variable.
        query = _replace_iter(
            query,
            r"(?P<first_char>[^_])datetime",
            "{first_char}timestamp",
            flags=re.IGNORECASE,
        )
        query = _replace_iter(query, r"int64", "integer", flags=re.IGNORECASE)
        query = _replace_iter(query, r"float64", "float", flags=re.IGNORECASE)

        # Postgres doesn't support the '* EXCEPT(...)' construct. There is really no
        # good way to suppport it so just ignore it.
        query = _replace_iter(query, r"\*\s+EXCEPT\s*\(.*\)", "*")

        query = self._rewrite_structs(query)

        # Postgres doesn't support IGNORE NULLS in window functions and there isn't a
        # straightforward way to implement it on top. This instead simply strips out the
        # IGNORE NULL fragment. Note this is a significant behavior change.
        query = _replace_iter(
            query,
            r"(?P<function>LEAD|LAG|FIRST_VALUE|LAST_VALUE|NTH_VALUE)\((?P<column>\w+?) IGNORE NULLS\)",
            "{function}({column})",
            flags=re.IGNORECASE,
        )

        return query

    def _rewrite_table_references(self, query: str) -> str:
        """Maps BQ table references to the underlying Postgres tables"""
        table_reference_regex = re.compile(
            r"`[\w-]+\.(?P<dataset_id>[\w-]+)\.(?P<table_id>[\w-]+)`"
        )
        for match in re.finditer(table_reference_regex, query):
            table_reference = match.group()
            dataset_id, table_id = match.groups()
            dataset_match = re.match(r"(us_[a-z]{2})_raw_data", dataset_id)
            if dataset_match:
                dataset_id = dataset_match.group(1)  # region_code
                table_id = table_id.lower()

            location = BigQueryAddress(dataset_id=dataset_id, table_id=table_id)
            if location not in self.mock_bq_to_postgres_tables:
                raise KeyError(
                    f"BigQuery location [{location}] not properly registered - must be "
                    f"created via create_mock_bq_table."
                )
            query = query.replace(
                table_reference, self.mock_bq_to_postgres_tables[location]
            )
        return query

    def _rewrite_unnest_with_offset(self, query: str) -> str:
        """UNNEST WITH OFFSET must be transformed significantly, and returns the ordinality instead of the offset."""
        # TODO(#5081): If we move dimensions to their own tables, we may be able to get rid of the unnest clauses as
        # well as this logic to rewrite them.

        # Postgres requires a table alias when aliasing the outputs of unnest and it must be unique for each unnest. We
        # just use the letters of the alphabet for this starting with 'a'.
        table_alias_name_generator = NameGenerator()
        with_offset_regex = re.compile(
            r",\s+UNNEST\((?P<colname>.+?)\) AS (?P<unnestname>\w+?) "
            r"WITH OFFSET (?P<offsetname>\w+?)(?P<end>\W)"
        )
        match = re.search(with_offset_regex, query)
        while match:
            query = query.replace(
                match[0],
                f"\nLEFT JOIN LATERAL UNNEST({match[1]}) "
                f"WITH ORDINALITY AS {next(table_alias_name_generator)}({match[2]}, {match[3]}) ON TRUE{match[4]}",
            )
            match = re.search(with_offset_regex, query)
        return query

    def _rewrite_structs(self, query: str) -> str:
        """Define STRUCTS as new composite types and cast the elements to that type.

        Postgres supports creating anonymous types with ROW but does not support naming their fields so we have to
        cast them to a type instead.
        Note: This only supports the `STRUCT<field_name field_type, ...>` syntax.
        """
        # TODO(#5081): If we move dimensions to their own tables, we may be able to get rid of the structs as well as
        # this logic to rewrite them.
        struct_regex = re.compile(r"STRUCT<(?P<types>.+)>\((?P<fields>.+?)\)")
        match = re.search(struct_regex, query)
        while match:
            type_name = next(self.type_name_generator)

            converted_fields = []
            # The fields are of the form "field1 type1, field2 type2, ..."
            # We have to parse them so that we can convert the types to postgres types.
            for field in match[1].split(","):
                name, field_type = field.strip().split(" ")
                if field_type == "string":
                    converted_type = "text"
                else:
                    converted_type = field_type
                converted_fields.append((name, converted_type))
            field_stanza = ", ".join(
                [f"{name} {field_type}" for name, field_type in converted_fields]
            )

            # Create the type at the start of the query
            query = f"CREATE TYPE {type_name} AS ({field_stanza});\n{query}"

            # Instead of building a STRUCT, use ROW and cast to our type
            query = query.replace(match[0], f"CAST(ROW({match[2]}) AS {type_name})")

            match = re.search(struct_regex, query)
        return query
