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
# ============================================================================

"""Define the ORM schema objects that map directly to the database,
for Justice Counts report-related entities.

Each object in the schema has a unique constraint, defining the set of columns needed to identify a unique row. These
could be used as primary keys for the objects. However, given the large set of columns used for many objects,
`ReportTableDefinition` in particular, this becomes quite cumbersome to use for ForeignKeys and results in a lot of
copying of columns to child tables. We instead use generated primary keys with this unique constraint to prevent bloat
and make joins easier.

If this causes issues with database serialization in the future, we can evaluate using these directly as primary keys
or adding `source.id` to the primary key of all objects and partitioning along that.
"""

import enum
from typing import TypeVar

from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.sql.sqltypes import ARRAY, Date, Enum, Integer, Numeric, String

from recidiviz.persistence.database.base_schema import JusticeCountsBase


class AcquisitionMethod(enum.Enum):
    SCRAPED = 'SCRAPED'
    UPLOADED = 'UPLOADED'
    MANUALLY_ENTERED = 'MANUALLY_ENTERED'


class MetricType(enum.Enum):
    """Various aspects of the criminal justice system that are measured, as defined by the Justice Counts Framework."""
    ADMISSIONS = 'ADMISSIONS'
    ARRESTS = 'ARRESTS'
    POPULATION = 'POPULATION'
    REVOCATIONS = 'REVOCATIONS'
    TERMINATIONS = 'TERMINATIONS'


class MeasurementType(enum.Enum):
    """How the metric over a given time window was reduced to a single point.

    This is not comprehensive and can grow as needed."""
    # Measurement at a single point in time.
    INSTANT = 'INSTANT'

    # Measurement averaged over the window.
    AVERAGE = 'AVERAGE'

    # Count of events that occurred over the window.
    DELTA = 'DELTA'

    # Count of events that occurred over the window, deduplicated based on the person associated with the event such
    # that only a single event for each person is counted. In the future if other deduplication-based counting
    # methodologies are discovered this can be expanded or new fields can be added to cover those.
    PERSON_BASED_DELTA = 'PERSON_BASED_DELTA'

class System(enum.Enum):
    """Part of the overall criminal justice system that this pertains to, as defined by the Justice Counts Framework."""
    LAW_ENFORCEMENT = 'LAW_ENFORCEMENT'
    COURT_PROCESSES = 'COURT_PROCESSES'
    CORRECTIONS = 'CORRECTIONS'


class Source(JusticeCountsBase):
    """A website or organization that publishes reports.

    It is not necessarily specific to a state or jurisdiction, but frequently is.
    """
    __tablename__ = 'source'

    id = Column(Integer, autoincrement=True)

    name = Column(String(255), nullable=False)

    __table_args__ = tuple([
        PrimaryKeyConstraint(id),
        UniqueConstraint(name)])

class Report(JusticeCountsBase):
    """A document that is published by a source that contains data pertaining to the Justice Counts Framework.
    """
    __tablename__ = 'report'

    id = Column(Integer, autoincrement=True)

    # The source that this report is published by.
    source_id = Column(Integer, nullable=False)
    # This distinguishes between the many types of reports that a single source may produce, e.g. a Daily Status
    # Report or Monthly Fact Sheet, that contain different information and are potentially fetched and parsed using
    # different logic.
    type = Column(String(255), nullable=False)
    # Identifies a specific instance of a given report type. It should be constructed such that it is unique within a
    # given report type and source. The combination of report type and instance is used when ingesting a report to
    # determine whether this is an update to an existing report or a new report. For PDF reports, this may simply be
    # the title of the document after some validation has been performed. For webpages it may need to be dynamically
    # generated.
    instance = Column(String(255), nullable=False)

    # The date the report was published.
    publish_date = Column(Date, nullable=False)
    # The method used to acquire the data (e.g. scraped).
    acquisition_method = Column(Enum(AcquisitionMethod), nullable=False)
    # TODO(#4485): Add a list of projects (e.g. Justice Counts, Spark) for which this data was ingested.

    __table_args__ = tuple([
        PrimaryKeyConstraint(id),
        UniqueConstraint(source_id, type, instance),
        ForeignKeyConstraint([source_id], [Source.id])])

    source = relationship(Source)


class ReportTableDefinition(JusticeCountsBase):
    """The definition for what a table within a report describes.
    """
    __tablename__ = 'report_table_definition'

    id = Column(Integer, autoincrement=True)

    system = Column(Enum(System))
    metric_type = Column(Enum(MetricType))
    measurement_type = Column(Enum(MeasurementType))

    # Any dimensions where the data in the table only accounts for a subset of values for that dimension. For instance,
    # a table for the population metric may only cover data for the prison population, not those on parole or
    # probation. In that case filters would include a filter on population type.
    filtered_dimensions = Column(ARRAY(String(255)))
    # The value for each dimension from the above array.
    filtered_dimension_values = Column(ARRAY(String(255)))
    # The dimensions that the metric is broken down by in the report table. Each cell in a table instance has a unique
    # combination of values for the aggregated dimensions. Dimensions are sorted deterministically within the array.
    aggregated_dimensions = Column(ARRAY(String(255)))

    __table_args__ = tuple([
        PrimaryKeyConstraint(id),
        UniqueConstraint(metric_type, measurement_type, filtered_dimensions,
                         filtered_dimension_values, aggregated_dimensions)])


class ReportTableInstance(JusticeCountsBase):
    """An instance of a table that contains an actual set of data points along a shared set of dimensions.

    It typically maps to a literal table with columns and rows in a report, but in some cases a literal table in the
    report may map to multiple tables as defined here.
    """
    __tablename__ = 'report_table_instance'

    id = Column(Integer, autoincrement=True)

    report_id = Column(Integer, nullable=False)
    report_table_definition_id = Column(Integer, nullable=False)

    # The window of time that values in this table cover, represented by a start date (inclusive) and an end date
    # (exclusive). The data could represent an instant measurement, where the start and end are equal, or a window, e.g.
    # ADP over the last month.
    time_window_start = Column(Date, nullable=False)
    time_window_end = Column(Date, nullable=False)

    # This field can be used to store any text that the source provides describing the methodology used to calculate
    # the data. This is stored on instances so that if it changes from report to report, we don't overwrite methodology
    # for prior instances.
    methodology = Column(String(255))

    __table_args__ = tuple([
        PrimaryKeyConstraint(id),
        # TODO(#4476): We need to include time window as part of the unique constraint in case there is data for the
        # same table definition that represents multiple time windows within a single report. To make this work with
        # updates, I think we will re-ingest all table instances for a particular report table definition in an updated
        # report.
        UniqueConstraint(report_id, report_table_definition_id, time_window_start, time_window_end),
        ForeignKeyConstraint([report_id], [Report.id]),
        ForeignKeyConstraint([report_table_definition_id], [ReportTableDefinition.id])])

    report = relationship(Report)
    report_table_definition = relationship(ReportTableDefinition)


class Cell(JusticeCountsBase):
    """A single data point within a table.
    """
    __tablename__ = 'cell'

    id = Column(Integer, autoincrement=True)

    report_table_instance_id = Column(Integer, nullable=False)
    aggregated_dimension_values = Column(ARRAY(String(255)), nullable=False)

    value = Column(Numeric, nullable=False)

    __table_args__ = tuple([
        PrimaryKeyConstraint(id),
        UniqueConstraint(report_table_instance_id, aggregated_dimension_values),
        ForeignKeyConstraint([report_table_instance_id], [ReportTableInstance.id])])

    report_table_instance = relationship(ReportTableInstance)

# As this is a TypeVar, it should be used when all variables within the scope of this type should have the same
# concrete class.
JusticeCountsDatabaseEntity = TypeVar('JusticeCountsDatabaseEntity', bound=JusticeCountsBase)
