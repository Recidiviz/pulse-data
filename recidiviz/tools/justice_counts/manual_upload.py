# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utilities for ingesting a report (as CSVs) into the Justice Counts database.

Example usage:
python -m recidiviz.tools.justice_counts.manual_upload \
    --manifest-file recidiviz/tests/tools/justice_counts/reports/report1/manifest.yaml \
    --project-id recidiviz-staging
python -m recidiviz.tools.justice_counts.manual_upload \
    --manifest-file recidiviz/tests/tools/justice_counts/reports/report1/manifest.yaml \
    --project-id recidiviz-staging \
    --app-url http://127.0.0.1:5000
"""

from abc import abstractmethod
import argparse
import datetime
import decimal
import enum
import logging
import os
import sys
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Type, Union
from typing import Dict, List, Optional, Set, Tuple, Type, Union
import webbrowser

import attr
import pandas
from sqlalchemy import cast
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import UniqueConstraint
import yaml

from recidiviz.common.date import DateRange, first_day_of_month, last_day_of_month
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem, GcsfsFileContentsHandle
from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.utils.yaml import YAMLDict
from recidiviz.utils import metadata

# Dimensions

# TODO(#4472) Refactor all dimensions out to a common justice counts directory.
class Dimension:
    """Each dimension is represented as a class that is used to hold the values for that dimension and perform any
    necessary validation. All dimensions are categorical. Those with a pre-defined set of values are implemented as
    enums. Others are classes with a single text field to hold any value, and are potentially normalized to a
    pre-defined set of values as a separate dimension.
    """
    @classmethod
    @abstractmethod
    def get(cls, value: str):
        """Create an instance of the dimension based on the given value."""

    @classmethod
    @abstractmethod
    def identifier(cls) -> str:
        """The globally unique identifier of this dimension, used when storing it in the database.

        E.g. 'metric/population/type' or 'global/raw/gender'.
        """

    # TODO(#4474): Once the manifest can define column name mappings, and the global dimensions (race, gender, facility,
    # etc.) have localized and normalized versions, this should be removed.
    @classmethod
    def alternative_names(cls) -> Set[str]:
        """The list of column names that map to this dimension.

        E.g. 'Gender'.
        """
        return set()

    @classmethod
    def all_names(cls) -> Set[str]:
        """Returns all names that can be used to refer to this dimension, including the identifier and alternatives."""
        return cls.alternative_names().union({cls.identifier()})

    @property
    @abstractmethod
    def dimension_value(self) -> str:
        """The value of this dimension instance.

        E.g. 'FEMALE' is a potential value for an instance of the 'global/raw/gender' dimension.
        """


class PopulationType(Dimension, enum.Enum):
    @classmethod
    def get(cls, value: str) -> 'PopulationType':
        return cls(value)

    @classmethod
    def identifier(cls) -> str:
        return 'metric/population/type'

    @property
    def dimension_value(self) -> str:
        return self.value

    PAROLE = 'PAROLE'
    PROBATION = 'PROBATION'
    PRISON = 'PRISON'


class Country(Dimension, enum.Enum):
    @classmethod
    def get(cls, value: str) -> 'Country':
        return cls(value)

    @classmethod
    def identifier(cls) -> str:
        return 'global/location/country'

    @property
    def dimension_value(self) -> str:
        return self.value

    US = 'US'


# TODO(#4472): Pull this out to a common place and add all states.
class State(Dimension, enum.Enum):
    @classmethod
    def get(cls, value: str) -> 'State':
        return cls(value)

    @classmethod
    def identifier(cls) -> str:
        return 'global/location/state'

    @property
    def dimension_value(self) -> str:
        return self.value

    US_CO = 'US_CO'
    US_MS = 'US_MS'
    US_TN = 'US_TN'


class County(Dimension, enum.Enum):
    @classmethod
    def get(cls, value: str) -> 'County':
        return cls(value)


    @classmethod
    def identifier(cls) -> str:
        return 'global/location/county'

    @property
    def dimension_value(self) -> str:
        return self.value

Location = Union[Country, State, County]

# TODO(#4473): Make this per jurisdiction
@attr.s(frozen=True)
class Facility(Dimension):
    name: str = attr.ib()

    @classmethod
    def get(cls, value: str) -> 'Facility':
        return cls(value)

    @classmethod
    def identifier(cls) -> str:
        return 'global/raw/facility'

    @classmethod
    def alternative_names(cls) -> Set[str]:
        return {'Facility'}

    @property
    def dimension_value(self) -> str:
        return self.name

# TODO(#4472): Use main Race enum and normalize values
@attr.s(frozen=True)
class Race(Dimension):
    value: str = attr.ib()

    @classmethod
    def get(cls, value: str) -> 'Race':
        return cls(value)

    @classmethod
    def identifier(cls) -> str:
        return 'global/raw/race'

    @classmethod
    def alternative_names(cls) -> Set[str]:
        return {'Race'}

    @property
    def dimension_value(self) -> str:
        return self.value

# TODO(#4472): Use main Gender enum and normalize values
@attr.s(frozen=True)
class Gender(Dimension):
    value: str = attr.ib()

    @classmethod
    def get(cls, value: str) -> 'Gender':
        return cls(value)

    @classmethod
    def identifier(cls) -> str:
        return 'global/raw/gender'

    @classmethod
    def alternative_names(cls) -> Set[str]:
        return {'Gender', 'Sex'}

    @property
    def dimension_value(self) -> str:
        return self.value

# TODO(#4473): Raise an error if there are conflicting dimension names
DIMENSIONS_BY_NAME = {name: dimension
                      for dimension in Dimension.__subclasses__()
                      for name in dimension.all_names()}

# Ingest Models
# TODO(#4472): Pull these out into the ingest directory, alongside existing ingest_info.

# Properties are used within the models to handle the conversion specific to each object. This is advantageous so that
# if and when, for instance, a new metric needs to be added, it is clear what conversion steps must be implemented. Any
# conversion that is not specific to a particular object, but instead a class of objects, e.g. all Metrics, is instead
# implemented within the persistence code itself.
class DateFormatType(enum.Enum):
    DATE = 'DATE'
    MONTH = 'MONTH'


DateFormatParserType = Callable[[str], datetime.date]


DATE_FORMAT_PARSERS: Dict[DateFormatType, DateFormatParserType] = {
    DateFormatType.DATE: datetime.date.fromisoformat,
    DateFormatType.MONTH: lambda text: datetime.datetime.strptime(text, "%Y-%m").date()
}


class MeasurementWindowType(enum.Enum):
    RANGE = 'RANGE'
    SNAPSHOT = 'SNAPSHOT'


# Currently we only expect one or two columns to be used to construct date ranges, but this can be expanded in the
# future if needed.
DateRangeConverterType = Union[
    Callable[[datetime.date], DateRange],
    Callable[[datetime.date, datetime.date], DateRange],
]


class RangeType(enum.Enum):
    @classmethod
    def get_or_default(cls, text: Optional[str]) -> 'RangeType':
        if text is None:
            return RangeType.CUSTOM
        return cls(text)

    CUSTOM = 'CUSTOM'
    MONTH = 'MONTH'


RANGE_CONVERTERS: Dict[RangeType, DateRangeConverterType] = {
    RangeType.CUSTOM: DateRange,
    RangeType.MONTH: DateRange.for_month_of_date,
}


class SnapshotType(enum.Enum):
    @classmethod
    def get_or_default(cls, text: Optional[str]) -> 'SnapshotType':
        if text is None:
            return SnapshotType.DAY
        return cls(text)

    DAY = 'DAY'
    FIRST_DAY_OF_MONTH = 'FIRST_DAY_OF_MONTH'
    LAST_DAY_OF_MONTH = 'LAST_DAY_OF_MONTH'


SNAPSHOT_CONVERTERS: Dict[SnapshotType, DateRangeConverterType] = {
    SnapshotType.DAY: DateRange.for_day,
    SnapshotType.FIRST_DAY_OF_MONTH: lambda date: DateRange.for_day(first_day_of_month(date)),
    SnapshotType.LAST_DAY_OF_MONTH: lambda date: DateRange.for_day(last_day_of_month(date)),
}


class Metric:
    @property
    @abstractmethod
    def filters(self) -> List[Dimension]:
        """Any dimensions where the data only represents a subset of values for that dimension.

        For instance, a table for the population metric may only cover data for the prison population, not those on
        parole or probation. In that case filters would contain PopulationType.PRISON.
        """

    @property
    @abstractmethod
    def measurement_type(self) -> schema.MeasurementType:
        """How the metric over a given time window was reduced to a single point."""

    @classmethod
    @abstractmethod
    def metric_type(cls) -> schema.MetricType:
        """The metric type that this corresponds to in the schema."""


# TODO(#4483): Add implementations for other metrics
@attr.s(frozen=True)
class Population(Metric):
    population_type: PopulationType = attr.ib(converter=PopulationType)

    @property
    def filters(self) -> List[Dimension]:
        return [self.population_type]

    @property
    def measurement_type(self) -> schema.MeasurementType:
        # TODO(#4484): Population isn't always instant, make this dynamic and straightforward to configure. Potentially
        # integrate with DateRange?
        return schema.MeasurementType.INSTANT

    @classmethod
    def metric_type(cls) -> schema.MetricType:
        return schema.MetricType.POPULATION

def _convert_dimensions(dimensions: Iterable[Tuple[Type[Dimension], str]]) -> List[Dimension]:
    return [dimension_type.get(dimension_value) for dimension_type, dimension_value in dimensions]

class DateRangeProducer:
    """Produces DateRanges for a given table, splitting the table as needed.
    """
    @abstractmethod
    def split_dataframe(self, df: pandas.DataFrame) -> List[Tuple[DateRange, pandas.DataFrame]]:
        pass

@attr.s(frozen=True, kw_only=True)
class FixedDateRangeProducer(DateRangeProducer):
    """Used when data in the table is for a single date range, configured outside of the table.
    """
    # The date range for the table
    fixed_range: DateRange = attr.ib()

    def split_dataframe(self, df: pandas.DataFrame) -> List[Tuple[DateRange, pandas.DataFrame]]:
        return [(self.fixed_range, df)]

@attr.s(frozen=True, kw_only=True)
class DynamicDateRangeProducer(DateRangeProducer):
    """Used when data in the table is for multiple date ranges, represented by the
    values of a particular set of columns in the table.
    """
    # The columns that contain the date ranges and how to parse the values in that column.
    # The parsed values are passed to `converter` in the same order in which the columns are specified in the dict.
    columns: Dict[str, DateFormatParserType] = attr.ib()
    # The function to use to convert the column values into date ranges
    converter: DateRangeConverterType = attr.ib()

    @property
    def column_names(self) -> List[str]:
        return list(self.columns.keys())

    @property
    def column_parsers(self) -> List[DateFormatParserType]:
        return list(self.columns.values())

    def split_dataframe(self, df: pandas.DataFrame) -> List[Tuple[DateRange, pandas.DataFrame]]:
        # - Groups the df by the specified column, getting a separate df per date range
        # - Converts the column values to a `DateRange`, using the provided converter
        # - Drops the columns from the split dfs, as they are no longer needed
        return [(self._convert(date_args), split.drop(self.column_names, axis=1))
                for date_args, split in df.groupby(self.column_names)]

    def _convert(self, args: Union[str, List[str]]) -> DateRange:
        unified_args: List[str] = [args] if isinstance(args, str) else args
        parsed_args: List[datetime.date] = [parser(arg) for arg, parser in zip(unified_args, self.column_parsers)]
        # pylint: disable=not-callable
        return self.converter(*parsed_args)

@attr.s(frozen=True)
class Table:
    """Ingest model that represents a table in a report"""
    date_range: DateRange = attr.ib()
    metric: Metric = attr.ib()
    system: schema.System = attr.ib(converter=schema.System)
    methodology: str = attr.ib()

    dimensions: List[Type[Dimension]] = attr.ib()
    data: List[Tuple[Tuple[Dimension, ...], decimal.Decimal]] = attr.ib()

    location: Optional[Location] = attr.ib()
    additional_filters: List[Dimension] = attr.ib()

    @classmethod
    def from_table(cls, date_range: DateRange, metric: Metric, system: str, methodology: str,
                   location: Optional[Location], additional_filters: List[Tuple[str, str]],
                   dimension_names: List[str], rows: List[Tuple[Tuple[str, ...], decimal.Decimal]]) -> 'Table':
        dimensions = [DIMENSIONS_BY_NAME[name] for name in dimension_names]

        data = []
        for dimension_values, data_value in rows:
            dimension_entries = _convert_dimensions(zip(dimensions, dimension_values))
            data.append((tuple(dimension_entries), data_value))

        return cls(date_range, metric, system, methodology, dimensions, data, location,
                   _convert_dimensions([(DIMENSIONS_BY_NAME[name], value) for name, value in additional_filters]))

    @classmethod
    def list_from_dataframe(cls, date_range_producer: DateRangeProducer, metric: Metric, system: str,
                            methodology: str, location: Optional[Location], additional_filters: List[Tuple[str, str]],
                            df: pandas.DataFrame) -> List['Table']:
        tables = []
        for date_range, df_date in date_range_producer.split_dataframe(df):
            dimension_names, rows = Table._transform_dataframe(df_date)
            tables.append(cls.from_table(date_range=date_range, metric=metric, system=system, methodology=methodology,
                                         location=location, additional_filters=additional_filters,
                                         dimension_names=dimension_names, rows=rows))
        return tables

    @staticmethod
    def _transform_dataframe(df: pandas.DataFrame):
        # All columns but the last contain dimension values, the last column has the data value.
        # TODO(#4474): Support mapping column names to dimensions and specifying which column contains the value, date
        # range in the manifest.
        dimension_names = df.columns.values[:-1]
        rows = []
        for row in df.values:
            dimension_values = tuple(row[:-1])
            cell_value = row[-1:].astype(decimal.Decimal)[0]
            rows.append((dimension_values, cell_value))
        return dimension_names, rows

    @property
    def filters(self) -> List[Dimension]:
        # TODO(#4473): Enforce sorting, naming scheme/organization, normalization of (additional filters, dimensions)
        filters = self.metric.filters + self.additional_filters
        if self.location is not None:
            filters.append(self.location)
        return filters

    @property
    def filtered_dimension_names(self) -> List[str]:
        return [filter.identifier() for filter in self.filters]

    @property
    def filtered_dimension_values(self) -> List[str]:
        return [filter.dimension_value for filter in self.filters]

    @property
    def aggregated_dimension_names(self) -> List[str]:
        # TODO(#4473): Enforce sorting, naming scheme/organization, normalization of (additional filters, dimensions)
        return [dimension.identifier() for dimension in self.dimensions]

    @property
    def cells(self) -> List[Tuple[List[str], decimal.Decimal]]:
        return [([dimension.dimension_value for dimension in row[0]], row[1]) for row in self.data]

    # TODO(#4473): Validate dimensions and data match.

    # TODO(#4473): Synthesize normalized dimensions and return those alongside existing dimensions.


@attr.s(frozen=True)
class Report:
    # Name of the website or organization that published the report, e.g. 'Mississippi Department of Corrections'
    source_name: str = attr.ib()
    # Distinguishes between the many types of reports that a single source may produce, e.g. 'Daily Status Report' or
    # 'Monthly Fact Sheet'
    report_type: str = attr.ib()
    # Identifies a specific instance of a report type, and should be unique within report type and source, e.g. 'August
    # 2020' for the August Monthly Fact Sheet.
    report_instance: str = attr.ib()

    tables: List[Table] = attr.ib()

    # The date the report was published, used to identify updated reports.
    publish_date: datetime.date = attr.ib(converter=datetime.date.fromisoformat)  # type: ignore[misc]

    # TODO(#4481): Add field to store URL the report was pulled from, or other text describing how it was acquired.


# Parsing Layer
# TODO(#4480): Pull this out to somewhere within ingest

def _parse_location(location_input: YAMLDict) -> Location:
    """Expects a dict with a single entry, e.g. `{'state': 'US_XX'}`"""
    if len(location_input) == 1:
        [[location_type, location_name]] = location_input.get().items()
        if location_type == 'country':
            return Country(location_name)
        if location_type == 'state':
            return State(location_name)
        if location_type == 'county':
            return County(location_name)
    raise ValueError(f"Invalid location, expected a dictionary with a single key that is one of ('country', 'state', "
                     f"'county') but received: {repr(location_input)}")

def _get_converter(range_type_input: str, range_converter_input: Optional[str] = None) -> DateRangeConverterType:
    range_type = MeasurementWindowType(range_type_input)
    if range_type is MeasurementWindowType.SNAPSHOT:
        return SNAPSHOT_CONVERTERS[SnapshotType.get_or_default(range_converter_input)]
    if range_type is MeasurementWindowType.RANGE:
        return RANGE_CONVERTERS[RangeType.get_or_default(range_converter_input)]
    raise ValueError(f"Enum case not handled for {range_type} when building converter.")

# TODO(#4480): Generalize these parsing methods, instead of creating one for each class. If value is a dict, pop it,
# find all implementing classes of `key`, find matching class, pass inner dict as parameters to matching class.
def _parse_date_range(range_input: YAMLDict) -> DateRange:
    """Expects a dict with a single entry that is a dict, e.g. `{'snapshot': {'date': '2020-11-01'}}`"""
    if len(range_input) == 1:
        [[range_type, range_args]] = range_input.get().items()
        converter = _get_converter(range_type.upper())
        if isinstance(range_args, dict):
            parsed_args = {key: datetime.date.fromisoformat(value) for key, value in range_args.items()}
            return converter(**parsed_args)  # type: ignore[call-arg]
    raise ValueError(f"Invalid date range, expected a dictionary with a single key that is one of but received: "
                        f"{repr(range_input)}")

def _parse_dynamic_date_range_producer(range_input: YAMLDict) -> DynamicDateRangeProducer:
    """Expects a dict with type (str), columns (dict) and converter (str, optional) entries.

    E.g. `{'type': 'SNAPSHOT' {'columns': 'Date': 'DATE'}}`
    """
    range_type = range_input.pop('type', str)
    range_converter = range_input.pop_optional('converter', str)
    column_names = range_input.pop('columns', dict)
    columns = {key: DATE_FORMAT_PARSERS[DateFormatType(value)] for key, value in column_names.items()}

    if len(range_input) > 0:
        raise ValueError(f"Received unexpected parameters for date range: {repr(range_input)}")

    return DynamicDateRangeProducer(converter=_get_converter(range_type, range_converter), columns=columns)

def _parse_date_range_producer(range_producer_input: YAMLDict) \
        -> DateRangeProducer:
    """Expects a dict with a single entry that is the arguments for the producer, e.g. `{'fixed': ...}`"""
    if len(range_producer_input) == 1:
        [range_producer_type] = range_producer_input.get().keys()
        range_producer_args = range_producer_input.pop_dict(range_producer_type)
        if range_producer_type == 'fixed':
            return FixedDateRangeProducer(fixed_range=_parse_date_range(range_producer_args))
        if range_producer_type == 'dynamic':
            return _parse_dynamic_date_range_producer(range_producer_args)
    raise ValueError(f"Invalid date range, expected a dictionary with a single key that is one of ('fixed', 'dynamic'"
                     f") but received: {repr(range_producer_input)}")

def _parse_metric(metric_input: YAMLDict) -> Metric:
    """Expects a dict with a single entry that is the arguments for the metric, e.g. `{'population': ...}`"""
    if len(metric_input) == 1:
        [metric_type] = metric_input.get().keys()
        metric_args = metric_input.pop(metric_type, dict)
        if metric_type == 'population':
            return Population(**metric_args)
    raise ValueError(f"Invalid metric, expected a dictionary with a single key that is one of ('population') but "
                     f"received: {repr(metric_input)}")


# Only three layers of dictionary nesting is currently supported by the table parsing logic but we use the recursive
# dictionary type for convenience.
def _parse_tables(gcs: GCSFileSystem, directory_path: GcsfsDirectoryPath, tables_input: List[YAMLDict]) -> List[Table]:
    """Parses the YAML list of dictionaries describing tables into Table objects"""
    tables = []
    for table_input in tables_input:
        # Parse nested objects separately
        date_range_producer = _parse_date_range_producer(table_input.pop_dict('date_range'))
        location: Location = _parse_location(table_input.pop_dict('location'))
        metric = _parse_metric(table_input.pop_dict('metric'))

        table_path = GcsfsFilePath.from_directory_and_file_name(directory_path, table_input.pop('file', str))
        logging.info('Reading table: %s', table_path)
        table_handle = gcs.download_to_temp_file(table_path)
        if table_handle is None:
            raise ValueError(f"Unable to download table from path: {table_path}")
        with table_handle.open() as table_file:
            df = pandas.read_csv(table_file)

        tables.extend(Table.list_from_dataframe(
            date_range_producer=date_range_producer, metric=metric, system=table_input.pop('system', str),
            methodology=table_input.pop('methodology', str), location=location,
            additional_filters=table_input.pop_optional('additional_filters', list) or [], df=df))

        if len(table_input) > 0:
            raise ValueError(f"Received unexpected parameters for table: {table_input}")

    return tables


def _get_report(gcs: GCSFileSystem, manifest_path: GcsfsFilePath) -> Report:
    logging.info('Reading report manifest: %s', manifest_path)
    manifest_handle = gcs.download_to_temp_file(manifest_path)
    if manifest_handle is None:
        raise ValueError(f"Unable to download manifest from path: {manifest_path}")
    with manifest_handle.open() as manifest_file:
        loaded_yaml = yaml.full_load(manifest_file)
        if not isinstance(loaded_yaml, dict):
            raise ValueError(f"Expected manifest to contain a top-level dictionary, but received: {loaded_yaml}")
        manifest = YAMLDict(loaded_yaml)

        directory_path = GcsfsDirectoryPath.from_file_path(manifest_path)
        # Parse tables separately
        # TODO(#4479): Also allow for location to be a column in the csv, as is done for dates.
        tables = _parse_tables(gcs, directory_path, manifest.pop_dicts('tables'))

        report = Report(
            source_name=manifest.pop('source', str),
            report_type=manifest.pop('report_type', str),
            report_instance=manifest.pop('report_instance', str),
            publish_date=manifest.pop('publish_date', str),
            tables=tables)

        if len(manifest) > 0:
            raise ValueError(f"Received unexpected parameters in manifest: {manifest}")

        return report

# Persistence Layer
# TODO(#4478): Refactor this into the persistence layer (including splitting out conversion, validation)

@attr.s(frozen=True)
class Metadata:
    acquisition_method: schema.AcquisitionMethod = attr.ib()


def _update_existing_or_create(ingested_entity: schema.JusticeCountsDatabaseEntity, session: Session) \
        -> schema.JusticeCountsDatabaseEntity:
    # Note: Using on_conflict_do_update to resolve whether there is an existing entity could be more efficient as it
    # wouldn't incur multiple roundtrips. However for some entities we need to know whether there is an existing entity
    # (e.g. table instance) so we can clear child entities, so we probably wouldn't win much if anything.
    table = ingested_entity.__table__
    [unique_constraint] = [constraint for constraint in table.constraints if isinstance(constraint, UniqueConstraint)]
    query = session.query(table)
    for column in unique_constraint:
        # TODO(#4477): Instead of making an assumption about how the property name is formed from the column name, use
        # an Entity method here to follow the foreign key relationship.
        if column.name.endswith('_id'):
            value = getattr(ingested_entity, column.name[:-len('_id')]).id
        else:
            value = getattr(ingested_entity, column.name)
        # Cast to the type because array types aren't deduced properly.
        query = query.filter(column == cast(value, column.type))
    table_entity: Optional[JusticeCountsBase] = query.first()
    if table_entity is not None:
        # TODO(#4477): Instead of assuming the primary key field is named `id`, use an Entity method.
        ingested_entity.id = table_entity.id
        # TODO(#4477): Merging here doesn't seem perfect, although it should work so long as the given entity always has
        # all the properties set explicitly. To avoid the merge, the method could instead take in the entity class as
        # one parameter and the parameters to construct it separately and then query based on those parameters. However
        # this would likely make mypy less useful.
        merged_entity = session.merge(ingested_entity)
        return merged_entity
    session.add(ingested_entity)
    return ingested_entity


def _convert_entities(session: Session, ingested_report: Report, report_metadata: Metadata) -> None:
    """Convert the ingested report into SQLAlchemy models"""
    report = _update_existing_or_create(schema.Report(
        source=_update_existing_or_create(schema.Source(name=ingested_report.source_name), session),
        type=ingested_report.report_type,
        instance=ingested_report.report_instance,
        publish_date=ingested_report.publish_date,
        acquisition_method=report_metadata.acquisition_method,
    ), session)

    for table in ingested_report.tables:
        table_definition = _update_existing_or_create(schema.ReportTableDefinition(
            system=table.system,
            metric_type=table.metric.metric_type(),
            measurement_type=table.metric.measurement_type,
            filtered_dimensions=table.filtered_dimension_names,
            filtered_dimension_values=table.filtered_dimension_values,
            aggregated_dimensions=table.aggregated_dimension_names,
        ), session)

        # TODO(#4476): Add ingested date to table_instance so that if we ingest a report update we can see which tables
        # are newer and prefer them.

        table_instance = _update_existing_or_create(schema.ReportTableInstance(
            report=report,
            report_table_definition=table_definition,
            time_window_start=table.date_range.lower_bound_inclusive_date,
            time_window_end=table.date_range.upper_bound_exclusive_date,
        ), session)

        # TODO(#4476): Clear any existing cells in the database for this report table instance. In the common case,
        # there won't be any, but if this is an update to a report, and the set of dimension combinations covered by
        # the new table is different, we want to make sure no stale data is left accidentally.
        for dimensions, value in table.cells:
            _update_existing_or_create(schema.Cell(
                report_table_instance=table_instance,
                aggregated_dimension_values=dimensions,
                value=value), session)


def _persist_report(report: Report, report_metadata: Metadata):
    session: Session = SessionFactory.for_schema_base(JusticeCountsBase)

    try:
        _convert_entities(session, report, report_metadata)
        # TODO(#4475): Add sanity check validation of the data provided (e.g. summing data across dimensions is
        # consistent). Validation of dimension values should already be enforced by enums above.
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def ingest(gcs: GCSFileSystem, manifest_filepath: GcsfsFilePath) -> None:
    logging.info('Fetching report for ingest...')
    report = _get_report(gcs, manifest_filepath)
    logging.info('Ingesting report...')
    _persist_report(report, Metadata(acquisition_method=schema.AcquisitionMethod.MANUALLY_ENTERED))
    logging.info('Report ingested.')

# TODO(#4127): Everything above should be refactored out of the tools directory so only the script below is left.

def _create_parser():
    """Creates the CLI argument parser."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--manifest-file', required=True, type=str,
        help="The yaml describing how to ingest the data"
    )
    parser.add_argument(
        '--project-id', required=True, type=str,
        help="The GCP project to ingest the data into"
    )
    parser.add_argument(
        '--app-url', required=False, type=str,
        help="Override the url of the app."
    )
    parser.add_argument(
        '--log', required=False, default='INFO', type=logging.getLevelName,
        help="Set the logging level"
    )
    return parser

def upload(gcs: GCSFileSystem, manifest_path: str) -> GcsfsFilePath:
    with open(manifest_path, mode='r') as manifest_file:
        directory = os.path.dirname(manifest_path)
        manifest: dict = yaml.full_load(manifest_file)

        gcs_directory = GcsfsDirectoryPath.from_absolute_path(
            os.path.join(f'gs://{metadata.project_id()}-justice-counts-ingest', manifest['source'],
                         manifest['report_type'], manifest['report_instance'])
        )

        for table in manifest['tables']:
            table_filename = table['file']
            gcs.upload_from_contents_handle(
                path=GcsfsFilePath.from_directory_and_file_name(gcs_directory, table_filename),
                contents_handle=GcsfsFileContentsHandle(os.path.join(directory, table_filename)),
                content_type='text/csv'
            )

        manifest_gcs_path = GcsfsFilePath.from_directory_and_file_name(gcs_directory, os.path.basename(manifest_path))
        gcs.upload_from_contents_handle(
            path=manifest_gcs_path,
            contents_handle=GcsfsFileContentsHandle(manifest_path),
            content_type='text/yaml'
        )
        return manifest_gcs_path

def trigger_ingest(gcs_path: GcsfsFilePath, app_url: Optional[str]):
    app_url = app_url or f'https://{metadata.project_id()}.appspot.com'
    webbrowser.open(url=f'{app_url}/justice_counts/ingest?manifest_path={gcs_path.uri()}')


def main(manifest_path: str, app_url: Optional[str]) -> None:
    logging.info('Uploading report for ingest...')
    gcs_path = upload(GcsfsFactory.build(), manifest_path)

    # We can't hit the endpoint on the app directly from the python script as we don't have IAP credentials. Instead we
    # launch the browser to hit the app and allow the user to auth in browser.
    logging.info('Opening browser to trigger ingest...')
    trigger_ingest(gcs_path, app_url)
    # Then we ask the user if the browser request was successful or displayed an error.
    i = input('Was the ingest successful? [Y/n]: ')
    if i and i.strip().lower() != 'y':
        logging.error('Ingest failed.')
        sys.exit(1)
    logging.info('Report ingested.')


def _configure_logging(level):
    root = logging.getLogger()
    root.setLevel(level)


if __name__ == '__main__':
    arg_parser = _create_parser()
    arguments = arg_parser.parse_args()

    _configure_logging(arguments.log)

    with metadata.local_project_id_override(arguments.project_id):
        main(arguments.manifest_file, arguments.app_url)
