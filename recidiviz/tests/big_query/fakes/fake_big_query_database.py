# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A database that can be used for testing BigQuery queries that uses Postgres as
a backing database.
"""
import logging
from typing import Dict, List, Optional, Type, Union

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions

from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
    BigQueryView,
    BigQueryViewBuilder,
)
from recidiviz.persistence.database.session import Session
from recidiviz.tests.big_query.fakes.big_query_query_rewriter import (
    BigQueryQueryRewriter,
)
from recidiviz.tests.big_query.fakes.fake_big_query_address_registry import (
    FakeBigQueryAddressRegistry,
)
from recidiviz.tests.big_query.fakes.fake_table_schema import MockTableSchema
from recidiviz.tools.postgres import local_postgres_helpers

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


# Adapted from: https://patternmatchers.wordpress.com/2021/06/11/ignore-nulls-in-postgres/
_CREATE_COALESCE_REVERSE_FUNC = """
CREATE FUNCTION coalesce_reverse (state anyelement, value anyelement)
    returns anyelement
    immutable parallel safe
as
$func$
SELECT COALESCE(value, state);
$func$ language sql;
"""

_DROP_COALESCE_REVERSE_FUNC = """
DROP FUNCTION coalesce_reverse(anyelement, anyelement)
"""

_CREATE_LAST_IGNORE_NULLS_FUNC = """
CREATE AGGREGATE last_value_ignore_nulls(anyelement) (
    sfunc = coalesce_reverse,
    stype = anyelement
);
"""

_DROP_LAST_IGNORE_NULLS_FUNC = """
DROP AGGREGATE last_value_ignore_nulls(anyelement)
"""

_CREATE_COALESCE_FORWARD_FUNC = """
CREATE FUNCTION coalesce_forward (state anyelement, value anyelement)
    returns anyelement
    immutable parallel safe
as
$func$
SELECT COALESCE(state, value);
$func$ language sql;
"""

_DROP_COALESCE_FORWARD_FUNC = """
DROP FUNCTION coalesce_forward(anyelement, anyelement)
"""

_CREATE_FIRST_IGNORE_NULLS_FUNC = """
CREATE AGGREGATE first_value_ignore_nulls(anyelement) (
    sfunc = coalesce_forward,
    stype = anyelement
);
"""

_DROP_FIRST_IGNORE_NULLS_FUNC = """
DROP AGGREGATE first_value_ignore_nulls(anyelement)
"""


class FakeBigQueryDatabase:
    """A database that can be used for testing BigQuery queries that uses Postgres as
    a backing database. TestCase classes using this class should integrate as follows:

    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.fake_bq_db = FakeBigQueryDatabase()

    def tearDown(self) -> None:
        self.fake_bq_db.teardown_databases()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )
    """

    def __init__(self) -> None:
        self._address_registry = FakeBigQueryAddressRegistry()
        self._query_rewriter = BigQueryQueryRewriter(self._address_registry)
        self._postgres_engine = create_engine(
            local_postgres_helpers.on_disk_postgres_db_url()
        )
        # Implement ARRAY_CONCAT_AGG function that behaves the same (ARRAY_AGG fails on empty arrays)
        self._execute_statement(_CREATE_ARRAY_CONCAT_AGG_FUNC)

        # Implement COND to behave like IF, as munging to case is error prone
        self._execute_statement(_CREATE_COND_FUNC)

        # Implement IGNORE NULLS for FIRST and LAST window functions.
        self._execute_statement(_CREATE_COALESCE_REVERSE_FUNC)
        self._execute_statement(_CREATE_COALESCE_FORWARD_FUNC)
        self._execute_statement(_CREATE_FIRST_IGNORE_NULLS_FUNC)
        self._execute_statement(_CREATE_LAST_IGNORE_NULLS_FUNC)

    def teardown_databases(self) -> None:
        close_all_sessions()

        # Execute each statement one at a time for resilience.
        for postgres_table_name in self._address_registry.all_postgres_tables():
            self._execute_statement(f"DROP TABLE IF EXISTS {postgres_table_name}")

        for type_name in self._query_rewriter.all_type_names_generated():
            self._execute_statement(f"DROP TYPE {type_name}")

        self._execute_statement(_DROP_ARRAY_CONCAT_AGG_FUNC)
        self._execute_statement(_DROP_COND_FUNC)
        self._execute_statement(_DROP_LAST_IGNORE_NULLS_FUNC)
        self._execute_statement(_DROP_FIRST_IGNORE_NULLS_FUNC)
        self._execute_statement(_DROP_COALESCE_REVERSE_FUNC)
        self._execute_statement(_DROP_COALESCE_FORWARD_FUNC)

        if self._postgres_engine is not None:
            self._postgres_engine.dispose()
            self._postgres_engine = None

    def _execute_statement(self, statement: str) -> None:
        session = Session(bind=self._postgres_engine)
        try:
            session.execute(statement)
            session.commit()
        except Exception as e:
            logging.warning("Failed to execute statement: %s\n%s", e, statement)
            session.rollback()
        finally:
            session.close()

    def create_mock_bq_table(
        self,
        dataset_id: str,
        table_id: str,
        mock_schema: MockTableSchema,
        mock_data: pd.DataFrame,
    ) -> None:
        postgres_table_name = self._address_registry.register_bq_address(
            address=BigQueryAddress(dataset_id=dataset_id, table_id=table_id)
        )

        mock_data.to_sql(
            name=postgres_table_name,
            con=self._postgres_engine,
            dtype=mock_schema.data_types,
            index=False,
        )

    def create_view(self, view_builder: BigQueryViewBuilder) -> None:
        view: BigQueryView = view_builder.build()
        table_location = view.table_for_query
        self._address_registry.register_bq_address(table_location)
        query = (
            f"CREATE TABLE "
            f"`{view.project}.{table_location.dataset_id}.{table_location.table_id}` "
            f"AS ({view.view_query})"
        )
        query = self._query_rewriter.rewrite_to_postgres(query)
        self._execute_statement(query)

        # Log results to debug log level, to see them pass --log-level DEBUG to pytest
        results = pd.read_sql_query(
            f"SELECT * FROM {self._address_registry.get_postgres_table(table_location)}",
            con=self._postgres_engine,
        )
        logging.debug("Results for `%s`:\n%s", table_location, results.to_string())

    def run_query(
        self,
        query_str: str,
        data_types: Optional[Union[Type, Dict[str, Type]]],
        dimensions: Optional[List[str]],
    ) -> pd.DataFrame:
        """Query the PG tables with the Postgres formatted query string and return the
        results as a DataFrame.
        """
        if not self._query_rewriter:
            raise ValueError(
                "Found null query rewriter - did you call setup_databases()?."
            )
        query_str = self._query_rewriter.rewrite_to_postgres(query_str)

        # TODO(#5533): Instead of using read_sql_query, we can use
        # `create_view` and `read_sql_table`. That can take a schema which will
        # solve some of the issues. As part of adding `dimensions` to builders
        # (below) we should likely just define a full output schema.
        results = pd.read_sql_query(query_str, con=self._postgres_engine)
        # If data types are not provided or all columns will be strings, transform 'nan'
        # values to empty strings. These occur when reading null values into a dataframe
        if not data_types or data_types == str:
            results = results.fillna("")
        if data_types:
            results = results.astype(data_types)
        # TODO(#5533): If we add `dimensions` to all `BigQueryViewBuilder`, instead of just
        # `MetricBigQueryViewBuilder`, then we can reuse that here instead of forcing
        # the caller to specify them manually.
        if dimensions:
            results = results.set_index(dimensions)
        results = results.sort_index()

        return results
