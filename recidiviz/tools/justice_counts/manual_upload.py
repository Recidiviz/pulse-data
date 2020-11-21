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
"""Utilities for ingesting a report (as CSVs) into the Justice Counts database. """

from abc import abstractmethod
import datetime
import decimal
import enum
import logging
from numbers import Number
import os
from typing import Callable, Dict, List, Optional, Set, Tuple, Type, Union

import attr
import pandas
from sqlalchemy import cast
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import UniqueConstraint
import yaml

from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.utils import types

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
    def identifier(cls) -> str:
        return 'global/location/country'

    @property
    def dimension_value(self) -> str:
        return self.value

    US = 'US'


# TODO(#4472): Pull this out to a common place and add all states.
class State(Dimension, enum.Enum):
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
    def identifier(cls) -> str:
        return 'global/raw/facility'

    @classmethod
    def alternative_names(cls) -> str:
        return {'Facility'}

    @property
    def dimension_value(self) -> str:
        return self.name

# TODO(#4472): Use main Race enum and normalize values
@attr.s(frozen=True)
class Race(Dimension):
    value: str = attr.ib()

    @classmethod
    def identifier(cls) -> str:
        return 'global/raw/race'

    @classmethod
    def alternative_names(cls) -> str:
        return {'Race'}

    @property
    def dimension_value(self) -> str:
        return self.value

# TODO(#4472): Use main Gender enum and normalize values
@attr.s(frozen=True)
class Gender(Dimension):
    value: str = attr.ib()

    @classmethod
    def identifier(cls) -> str:
        return 'global/raw/gender'

    @classmethod
    def alternative_names(cls) -> str:
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

# TODO(#4482): Consolidate the TimeWindow implementations and use the existing DateRange class. Create additional
# convenience factory methods to be used as converters.
# * FIRST_OF_MONTH: takes a month str, gives snapshot on first of month
# * MONTH: takes a month str, gives range for month
# ...
class TimeWindowType(enum.Enum):
    SNAPSHOT = 'SNAPSHOT'
    RANGE = 'RANGE'

class TimeWindow:
    """The window of time that values in this table cover, represented by a start and an end.

    The data could represent an instant measurement, where the start and end are equal, or a range, e.g. ADP over the
    last month.
    """
    @property
    @abstractmethod
    def start(self) -> datetime.date:
        """The start of the window that the given data covers, inclusive.

        For example, if the data is an average over the month of August 2020, this would be 2020-08-01.
        """

    @property
    @abstractmethod
    def end(self) -> datetime.date:
        """The end of the window that the given data covers, exclusive.

        For example, if the data is an average over the month of August 2020, this would be 2020-09-01.
        """


@attr.s(frozen=True)
class Snapshot(TimeWindow):
    date: datetime.date = attr.ib(converter=datetime.date.fromisoformat)

    @property
    def start(self) -> datetime.date:
        return self.date

    @property
    def end(self) -> datetime.date:
        return self.date + datetime.timedelta(days=1)


@attr.s(frozen=True)
class Range(TimeWindow):
    from_date: datetime.date = attr.ib(converter=datetime.date.fromisoformat)
    to_date: datetime.date = attr.ib(converter=datetime.date.fromisoformat)

    @property
    def start(self) -> datetime.date:
        return self.from_date

    @property
    def end(self) -> datetime.date:
        return self.to_date


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
        # integrate with TimeWindow?
        return schema.MeasurementType.INSTANT

    @classmethod
    def metric_type(cls) -> schema.MetricType:
        return schema.MetricType.POPULATION

def _convert_dimensions(dimensions: List[Tuple[str, str]]) -> List[Dimension]:
    return [dimension_type(dimension_value) for dimension_type, dimension_value in dimensions]


# Currently we only expect one or two columns to be used to construct time windows, but this can be expanded in the
# future if needed.
TimeWindowConverterType = Union[
    Callable[[str], TimeWindow],
    Callable[[str, str], TimeWindow],
]

class TimeWindowProducer:
    """Produces TimeWindows for a given table, splitting the table as needed.
    """
    @abstractmethod
    def split_dataframe(self, df: pandas.DataFrame) -> List[Tuple[TimeWindow, pandas.DataFrame]]:
        pass

@attr.s(frozen=True, kw_only=True)
class FixedTimeWindowProducer(TimeWindowProducer):
    """Used when data in the table is for a single time window, configured outside of the table.
    """
    # The time window for the table
    fixed_window: TimeWindow = attr.ib()

    def split_dataframe(self, df: pandas.DataFrame) -> List[Tuple[TimeWindow, pandas.DataFrame]]:
        return [(self.fixed_window, df)]

@attr.s(frozen=True, kw_only=True)
class DynamicTimeWindowProducer(TimeWindowProducer):
    """Used when data in the table is for multiple time windows, represented by the
    values of a particular set of columns in the table.
    """
    # The columns that contain the time windows
    column_names: List[str] = attr.ib()
    # The function to use to convert the column values into time windows
    converter: TimeWindowConverterType = attr.ib()

    def split_dataframe(self, df: pandas.DataFrame) -> List[Tuple[TimeWindow, pandas.DataFrame]]:
        # - Groups the df by the time column, getting a separate df per time
        # - Converts the value in the time column to a `TimeWindow`, using the provided converter
        # - Drops the time column from the split dfs, as it is no longer needed
        return [(self._convert(time_args), split.drop(self.column_names, axis=1))
                for time_args, split in df.groupby(self.column_names)]

    def _convert(self, args: Union[str, List[str]]) -> TimeWindow:
        args: List[str] = [args] if isinstance(args, str) else args
        # pylint: disable=not-callable
        return self.converter(*args)

@attr.s(frozen=True)
class Table:
    """Ingest model that represents a table in a report"""
    time_window: TimeWindow = attr.ib()
    metric: Metric = attr.ib()
    system: schema.System = attr.ib(converter=schema.System)
    methodology: str = attr.ib()

    dimensions: List[Type[Dimension]] = attr.ib()
    data: List[Tuple[Tuple[Dimension, ...], decimal.Decimal]] = attr.ib()

    location: Optional[Location] = attr.ib()
    additional_filters: List[Dimension] = attr.ib()

    @classmethod
    def from_table(cls, time_window: TimeWindow, metric: Metric, system: str, methodology: str,
                   location: Optional[Location], additional_filters: List[Tuple[str, str]],
                   dimension_names: List[str], rows: List[Tuple[Tuple[str, ...], Number]]) -> 'Table':
        dimensions = [DIMENSIONS_BY_NAME[identifier] for identifier in dimension_names]

        data = []
        for dimension_values, data_value in rows:
            dimension_entries = _convert_dimensions(zip(dimensions, dimension_values))
            data.append((tuple(dimension_entries), decimal.Decimal(data_value)))

        return cls(time_window, metric, system, methodology, dimensions, data, location,
                   _convert_dimensions(additional_filters))

    @classmethod
    def list_from_dataframe(cls, time_window_producer: TimeWindowProducer, metric: Metric, system: str,
                            methodology: str, location: Optional[Location], additional_filters: List[Tuple[str, str]],
                            df: pandas.DataFrame) -> List['Table']:
        tables = []
        for time_window, df_time in time_window_producer.split_dataframe(df):
            dimension_names, rows = Table._transform_dataframe(df_time)
            tables.append(cls.from_table(time_window=time_window, metric=metric, system=system, methodology=methodology,
                                         location=location, additional_filters=additional_filters,
                                         dimension_names=dimension_names, rows=rows))
        return tables

    @staticmethod
    def _transform_dataframe(df: pandas.DataFrame):
        # All columns but the last contain dimension values, the last column has the data value.
        # TODO(#4474): Support mapping column names to dimensions and specifying which column contains the value, time
        # window in the manifest.
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
        return ([self.location] or []) + self.metric.filters + self.additional_filters

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
    publish_date: datetime.date = attr.ib(converter=datetime.date.fromisoformat)

    # TODO(#4481): Add field to store URL the report was pulled from, or other text describing how it was acquired.


# Parsing Layer
# TODO(#4480): Pull this out to somewhere within ingest

def _parse_location(location_input: Dict[str, str]) -> Location:
    if isinstance(location_input, dict) and len(location_input) == 1:
        [[location_type, location_name]] = location_input.items()
        if location_type == 'country':
            return Country(location_name)
        if location_type == 'state':
            return State(location_name)
        if location_type == 'county':
            return County(location_name)
    raise ValueError(f"Invalid location, expected a dictionary with a single key that is one of ('country', 'state', "
                     f"'county') but received: {repr(location_input)}")

def _get_converter(window_type_input: str) -> TimeWindowConverterType:
    window_type = TimeWindowType(window_type_input)
    if window_type is TimeWindowType.SNAPSHOT:
        return Snapshot
    if window_type is TimeWindowType.RANGE:
        return Range
    raise ValueError(f"Enum case not handled for {window_type} when building converter.")

# TODO(#4480): Generalize these parsing methods, instead of creating one for each class. If value is a dict, pop it,
# find all implementing classes of `key`, find matching class, pass inner dict as parameters to matching class.
FixedTimeWindowYAML = Dict[str, Dict[str, str]]
def _parse_time_window(window_input: FixedTimeWindowYAML) -> TimeWindow:
    if isinstance(window_input, dict) and len(window_input) == 1:
        [[window_type, window_args]] = window_input.items()
        return _get_converter(window_type.upper())(**window_args)
    raise ValueError(f"Invalid time window, expected a dictionary with a single key that is one of but received: "
                     f"{repr(window_input)}")

DynamicTimeWindowYAML = Dict[str, Union[str, List[str]]]
def _parse_dynamic_time_window_producer(window_input: DynamicTimeWindowYAML) -> DynamicTimeWindowProducer:
    window_type: Optional[str] = window_input.pop('type', None)
    if not window_type or not isinstance(window_type, str):
        raise ValueError(f"Invalid column time window, expected key 'type' to have value ('SNAPSHOT', 'RANGE') "
                         f"in input: {repr(window_input)}")

    column_names: Optional[List[str]] = window_input.pop('names', None)
    if not column_names or not isinstance(column_names, list):
        raise ValueError(f"Invalid column time window, expected key 'names' to have non-empty list of column names in "
                         f"input: {repr(window_input)}.")

    if len(window_input) > 0:
        raise ValueError(f"Received unexpected parameters for time window: {repr(window_input)}")

    return DynamicTimeWindowProducer(converter=_get_converter(window_type), column_names=column_names)

def _parse_time_window_producer(window_producer_input: Dict[str, Union[FixedTimeWindowYAML, DynamicTimeWindowYAML]]) \
        -> TimeWindowProducer:
    if isinstance(window_producer_input, dict) and len(window_producer_input) == 1:
        [[window_producer_type, window_producer_args]] = window_producer_input.items()
        if window_producer_type == 'fixed':
            return FixedTimeWindowProducer(fixed_window=_parse_time_window(window_producer_args))
        if window_producer_type == 'column':
            return _parse_dynamic_time_window_producer(window_producer_args)
    raise ValueError(f"Invalid time window, expected a dictionary with a single key that is one of ('fixed', 'column'"
                     f") but received: {repr(window_producer_input)}")

def _parse_metric(metric_input: Dict[str, Dict[str, Union[str, float]]]) -> Metric:
    if isinstance(metric_input, dict) and len(metric_input) == 1:
        [[metric_type, metric]] = metric_input.items()
        if metric_type == 'population':
            return Population(**metric)
    raise ValueError(f"Invalid metric, expected a dictionary with a single key that is one of ('population') but "
                     f"received: {repr(metric_input)}")


# Only three layers of dictionary nesting is currently supported by the table parsing logic but we use the recursive
# dictionary type for convenience.
def _parse_tables(directory: str, tables_input: List[types.SimpleYAMLDict]) -> List[Table]:
    """Parses the YAML list of dictionaries describing tables into Table objects"""
    tables = []
    for table_input in tables_input:
        # Parse nested objects separately
        time_window_producer = _parse_time_window_producer(table_input.pop('time_window'))
        location: Location = _parse_location(table_input.pop('location'))
        metric = _parse_metric(table_input.pop('metric'))

        table_filepath = os.path.join(directory, table_input.pop('file'))
        logging.info('Reading table: %s', table_filepath)
        df = pandas.read_csv(table_filepath)

        tables.extend(Table.list_from_dataframe(
            time_window_producer=time_window_producer, metric=metric, system=table_input.pop('system'),
            methodology=table_input.pop('methodology'), location=location,
            additional_filters=table_input.pop('additional_filters', []), df=df))

        if len(table_input) > 0:
            raise ValueError(f"Received unexpected parameters for table: {table_input}")

    return tables


def _get_report(manifest_filepath):
    logging.info('Reading report manifest: %s', manifest_filepath)
    with open(manifest_filepath) as manifest_file:
        directory = os.path.dirname(manifest_filepath)
        manifest: dict = yaml.full_load(manifest_file)

        # Parse tables separately
        # TODO(#4479): Also allow for location to be a column in the csv, much like should be done for time above.
        tables = _parse_tables(directory, manifest.pop('tables'))

        report = Report(
            source_name=manifest.pop('source'),
            report_type=manifest.pop('report_type'),
            report_instance=manifest.pop('report_instance'),
            publish_date=manifest.pop('publish_date'),
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


def _convert_entities(session: Session, ingested_report: Report, metadata: Metadata) -> None:
    """Convert the ingested report into SQLAlchemy models"""
    report = _update_existing_or_create(schema.Report(
        source=_update_existing_or_create(schema.Source(name=ingested_report.source_name), session),
        type=ingested_report.report_type,
        instance=ingested_report.report_instance,
        publish_date=ingested_report.publish_date,
        acquisition_method=metadata.acquisition_method,
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
            time_window_start=table.time_window.start,
            time_window_end=table.time_window.end,
        ), session)

        # TODO(#4476): Clear any existing cells in the database for this report table instance. In the common case,
        # there won't be any, but if this is an update to a report, and the set of dimension combinations covered by
        # the new table is different, we want to make sure no stale data is left accidentally.
        for dimensions, value in table.cells:
            _update_existing_or_create(schema.Cell(
                report_table_instance=table_instance,
                aggregated_dimension_values=dimensions,
                value=value), session)


def _persist_report(report: Report, metadata: Metadata):
    session: Session = SessionFactory.for_schema_base(JusticeCountsBase)

    try:
        _convert_entities(session, report, metadata)
        # TODO(#4475): Add sanity check validation of the data provided (e.g. summing data across dimensions is
        # consistent). Validation of dimension values should already be enforced by enums above.
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def ingest(manifest_filepath: str) -> None:
    logging.info('Fetching report for ingest...')
    report = _get_report(manifest_filepath)
    logging.info('Ingesting report...')
    _persist_report(report, Metadata(acquisition_method=schema.AcquisitionMethod.MANUALLY_ENTERED))
    logging.info('Report ingested.')
