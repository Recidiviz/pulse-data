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
"""Stores functionality for rewriting BigQuery SQL queries with syntax that will
run in Postgres.
"""

import re
from typing import Iterator, List

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.tests.big_query.fakes.fake_big_query_address_registry import (
    FakeBigQueryAddressRegistry,
)
from recidiviz.utils.string import StrictStringFormatter

_INITIAL_SUFFIX_ASCII = ord("a")


def _replace_iter(query: str, regex: str, replacement: str, flags: int = 0) -> str:
    """Iteratively replaces subsstrings in |query| matching |regex| with |replacement|."""
    compiled = re.compile(regex, flags)
    for match in re.finditer(compiled, query):
        grouped_matches = {k: str(v or "") for k, v in dict(match.groupdict()).items()}
        query = query.replace(
            match[0],
            StrictStringFormatter().format(replacement, **grouped_matches),
        )
    return query


class _TypeNameGenerator(Iterator[str]):
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


class BigQueryQueryRewriter:
    """Stores functionality for rewriting BigQuery SQL queries with syntax that will
    run in Postgres.
    """

    def __init__(self, address_registry: FakeBigQueryAddressRegistry) -> None:
        self._type_name_generator = _TypeNameGenerator("__type_")
        self.address_registry = address_registry

    def all_type_names_generated(self) -> List[str]:
        return self._type_name_generator.all_names_generated()

    def rewrite_to_postgres(self, query: str) -> str:
        """Modifies the SQL query, translating BQ syntax to Postgres syntax where
        necessary.
        """
        query = self._rewrite_table_references(query)
        query = self._rewrite_unnest_with_offset(query)

        # Replace '#' comments with '--'
        query = _replace_iter(query, r"#", "--")

        # Replace timestamp format strings like '%m/%d/%y' to match PG 'MM/DD/YYYY'
        query = self._rewrite_timestamp_formats(query)

        # Replace date format strings like '%Y%m%d' to match PG 'YYYYMMDD'
        query = self._rewrite_date_formats(query)

        # Changes references to SAFE.PARSE_DATETIME to drop the SAFE, which is invalid in Postgres
        query = self._remove_safe_schema_from_parse_datetime(query)

        # Update % signs to be double escaped
        query = _replace_iter(
            query,
            r"(?P<leading_char>[^\%])\%(?P<trailing_char>[^\%])",
            "{leading_char}%%{trailing_char}",
        )

        # SPLIT function has different format
        query = _replace_iter(
            query,
            r"SPLIT\((?P<first>[^,]+?), (?P<delimiter>.+?)\)\[OFFSET\((?P<offset>.+?)\)\]",
            "split_part({first}, {delimiter}, {offset})",
        )
        query = _replace_iter(
            query,
            r"SPLIT\((?P<first>[^,]+?), (?P<delimiter>.+?)\)",
            "regexp_split_to_array({first}, {delimiter})",
        )

        # Must index the array directly, instead of using OFFSET or ORDINAL
        query = _replace_iter(query, r"\[OFFSET\((?P<offset>.+?)\)\]", "[{offset}]")
        query = _replace_iter(
            query, r"\[SAFE_OFFSET\((?P<offset>.+?)\)\]", "[{offset}]"
        )
        query = _replace_iter(query, r"\[ORDINAL\((?P<ordinal>.+?)\)\]", "[{ordinal}]")
        query = _replace_iter(
            query, r"\[SAFE_ORDINAL\((?P<ordinal>.+?)\)\]", "[{ordinal}]"
        )

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
        # replace `DATE(` with `make_date(`, but not `(DATE(`
        query = _replace_iter(
            query,
            r"(^| )DATE\s*\((?!PARSE_TIMESTAMP\()",
            " make_date(",
            flags=re.IGNORECASE,
        )

        # Postgres doesn't have DATE_DIFF where you can specify the units to return, but subtracting two dates always
        # returns the number of days between them.
        query = _replace_iter(
            query,
            r"DATE_DIFF\((?P<first>.+?), (?P<second>.+?), DAY\)",
            "({first}::date - {second}::date)",
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

        # Replace BigQuery case insensitive flag which does not exist in PG
        query = _replace_iter(query, r"\(\?i\)", "")

        # Remove the optional, additional `r` in REGEXP_REPLACE.
        # Append the flag `g` when doing REGEXP_REPLACE, otherwise
        # only the first match will be replaced.
        query = _replace_iter(
            query,
            r"REGEXP_REPLACE\((?P<value>.+), r?\'(?P<regex>[^']+)\', \'(?P<replacement>[^']*)\'\)",
            "REGEXP_REPLACE({value}, '{regex}', '{replacement}', 'g')",
        )

        # The REGEXP_CONTAINS function does not exist in postgres, so we replace with
        # 'REGEXP_MATCH(text, pattern) IS NOT NULL', which has the same behavior.
        query = _replace_iter(
            query,
            r"REGEXP_CONTAINS\((?P<first>.+?), r?(?P<second>.+?\)?')\)",
            "REGEXP_MATCH({first}, {second}) IS NOT NULL",
        )

        # EXTRACT returns a double in postgres, but for all part types shared between
        # the two, bigquery returns an int
        query = _replace_iter(
            query,
            r"EXTRACT\((?!DATE|DATETIME|TIME)(?P<clause>.+)\)(?P<end>[^:])",
            "EXTRACT({clause})::integer{end}",
        )

        # EXTRACT DATE returns a DATE type in BQ, this strips out the EXTRACT(DATE FROM timestamp_col) AS xx_column
        # and becomes timestamp_col::date
        query = _replace_iter(
            query,
            r"EXTRACT\(DATE\sFROM\s(?P<clause>\S+)\)(?P<end>.+)?",
            "{clause}::date{end}",
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
            r"(SAFE\.)?PARSE_TIMESTAMP\((?P<fmt>.+?), (?P<col>.+?\)?)\)",
            "TO_TIMESTAMP({col}, {fmt})",
            flags=re.IGNORECASE,
        )

        query = _replace_iter(
            query,
            r"(SAFE\.)?PARSE_DATETIME\((?P<fmt>.+?), (?P<col>.+?\)?)\)",
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

        # Postgres doesn't support COUNTIF()
        query = _replace_iter(
            query, r"COUNTIF\((?P<value>.+?)\)", "COUNT({value} OR null)"
        )

        query = _replace_iter(query, r"IFNULL", "COALESCE", flags=re.IGNORECASE)
        query = _replace_iter(query, r"(^| )IF\s*\(", " COND(", flags=re.IGNORECASE)

        # Replace DATETIME type with TIMESTAMP, attempting to not pick up the term
        # 'datetime' when used in a variable.
        query = _replace_iter(
            query,
            r"(?P<first_char>[^_A-Za-z])datetime",
            "{first_char}timestamp",
            flags=re.IGNORECASE,
        )

        # Replace timestamp(2020,1,1,0,0,0,0) calls from query diffs
        query = _replace_iter(
            query,
            r"timestamp\((?P<year>\d{4}), (?P<month>\d{0,2}), (?P<day>\d{0,2}), .+\)",
            "timestamp '{year}-{month}-{day}'",
            flags=re.IGNORECASE,
        )
        #  Replace TIMESTAMP('9999-12-31') calls
        query = _replace_iter(
            query,
            r"TIMESTAMP\('(?P<year>\d{4})-(?P<month>\d{0,2})-(?P<day>\d{0,2})'\)",
            "timestamp '{year}-{month}-{day}'",
            flags=re.IGNORECASE,
        )

        query = _replace_iter(query, r"int64", "integer", flags=re.IGNORECASE)
        query = _replace_iter(query, r"float64", "float", flags=re.IGNORECASE)

        # Postgres doesn't support the '* EXCEPT(...)' construct. There is really no
        # good way to suppport it so just ignore it.
        query = _replace_iter(query, r"\*\s+EXCEPT\s*\(.*\)", "*")

        query = self._rewrite_structs(query)

        # Postgres doesn't support IGNORE NULLS in window functions so we replace with
        # our own custom implementations.
        query = _replace_iter(
            query,
            r"FIRST_VALUE\((?P<column>\w+?) IGNORE NULLS\)",
            "first_value_ignore_nulls({column})",
            flags=re.IGNORECASE,
        )

        query = _replace_iter(
            query,
            r"LAST_VALUE\((?P<column>\w+?) IGNORE NULLS\)",
            "last_value_ignore_nulls({column})",
            flags=re.IGNORECASE,
        )

        # Allow trailing commas in SELECT.
        query = _replace_iter(query, r",(?P<whitespace>\s+)FROM", "{whitespace}FROM")

        # Postgres does not have LOGICAL_OR operator, use BOOL_OR instead
        query = _replace_iter(query, r"LOGICAL_OR", "BOOL_OR")

        # Postgres does not have LOGICAL_AND operator, use BOOL_AND instead
        query = _replace_iter(query, r"LOGICAL_AND", "BOOL_AND")

        # Postgres doesn't support TO_JSON_STRING so we replace with ARRAY_TO_JSON
        query = _replace_iter(
            query,
            r"TO_JSON_STRING\(",
            "ARRAY_TO_JSON(",
            flags=re.IGNORECASE,
        )
        return query

    BQ_TIMESTAMP_DATE_FORMAT_TO_POSTGRES = {
        "%m": "MM",
        "%Y": "YYYY",
        "%d": "DD",
        "%r": "HH:MI:SS AM",
        "%H": "HH24",
        "%I": "HH12",
        "%M": "MI",
        "%S": "SS",
        "%b": "Mon",
        "%e": "FMDD",
        "%p": "AM",
    }

    def _rewrite_timestamp_formats(self, query: str) -> str:
        """Timestamp format strings supported in Postgres have different structure than
        those supported in BigQuery. This function rewrites format strings
        (e.g. %Y-%m-%d .. etc) to a format supported by Postgres.
        """
        timestamp_format_pattern = re.compile(r"TIMESTAMP\((?P<timestamp_format>'%.+')")

        for match in re.finditer(timestamp_format_pattern, query):
            for item in match.groups():
                new_format = item
                for (
                    bq_format,
                    pg_format,
                ) in self.BQ_TIMESTAMP_DATE_FORMAT_TO_POSTGRES.items():
                    new_format = new_format.replace(bq_format, pg_format)

                query = query.replace(item, new_format)
        return query

    def _rewrite_date_formats(self, query: str) -> str:
        """Date format strings supported in Postgres have different structure than
        those supported in BigQuery. This function rewrites format strings
        (e.g. %Y-%m-%d .. etc) to a format supported by Postgres.
        """
        date_format_pattern = re.compile(r"DATE\((?P<date_format>'%.+')")

        for match in re.finditer(date_format_pattern, query):
            for item in match.groups():
                new_format = item
                for (
                    bq_format,
                    pg_format,
                ) in self.BQ_TIMESTAMP_DATE_FORMAT_TO_POSTGRES.items():
                    new_format = new_format.replace(bq_format, pg_format)

                query = query.replace(item, new_format)
        return query

    @staticmethod
    def _remove_safe_schema_from_parse_datetime(query: str) -> str:
        """The `SAFE` schema in BigQuery does not exist in Postgres - strips it off the
        `PARSE_DATETIME` function.
        """
        query = query.replace("SAFE.PARSE_DATETIME", "PARSE_DATETIME")
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
            postgres_table_name = self.address_registry.get_postgres_table(location)
            query = query.replace(table_reference, postgres_table_name)
        return query

    @staticmethod
    def _rewrite_unnest_with_offset(query: str) -> str:
        """UNNEST WITH OFFSET must be transformed significantly, and returns the
        ordinality instead of the offset."""
        # TODO(#5081): If we move dimensions to their own tables, we may be able to
        #  get rid of the unnest clauses as well as this logic to rewrite them.

        # Postgres requires a table alias when aliasing the outputs of unnest and it
        # must be unique for each unnest. We just use the letters of the alphabet for
        # this starting with 'a'.
        table_alias_name_generator = _TypeNameGenerator()
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

        Postgres supports creating anonymous types with ROW but does not support naming
        their fields so we have to cast them to a type instead.
        Note: This only supports the `STRUCT<field_name field_type, ...>` syntax.
        """
        # TODO(#5081): If we move dimensions to their own tables, we may be able to get
        #  rid of the structs as well as this logic to rewrite them.
        struct_regex = re.compile(r"STRUCT<(?P<types>.+)>\((?P<fields>.+?)\)")
        match = re.search(struct_regex, query)
        while match:
            type_name = next(self._type_name_generator)

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
