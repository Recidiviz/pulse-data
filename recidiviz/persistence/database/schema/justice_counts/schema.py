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
import re
from typing import Any, Dict, List, Optional, TypeVar

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeMeta,
    backref,
    declarative_base,
    relationship,
    validates,
)
from sqlalchemy.sql.schema import (
    Column,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy.sql.sqltypes import (
    ARRAY,
    Boolean,
    Date,
    DateTime,
    Enum,
    Integer,
    Numeric,
    String,
)

from recidiviz.common import fips
from recidiviz.common.constants.justice_counts import ValueType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.database_entity import DatabaseEntity

# Defines the base class for all table classes in the justice counts schema.
JusticeCountsBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="JusticeCountsBase"
)


class AcquisitionMethod(enum.Enum):
    SCRAPED = "SCRAPED"
    UPLOADED = "UPLOADED"
    MANUALLY_ENTERED = "MANUALLY_ENTERED"
    CONTROL_PANEL = "CONTROL_PANEL"


class MetricType(enum.Enum):
    """Various aspects of the criminal justice system that are measured, as defined by the Justice Counts Framework."""

    ADMISSIONS = "ADMISSIONS"
    ARRESTS = "ARRESTS"
    ARRESTS_ON_PRETRIAL_RELEASE = "ARRESTS_ON_PRETRIAL_RELEASE"
    BUDGET = "BUDGET"
    CALLS_FOR_SERVICE = "CALLS_FOR_SERVICE"
    CASELOADS = "CASELOADS"
    CASES_APPOINTED_COUNSEL = "CASES_APPOINTED_COUNSEL"
    CASES_DECLINED = "CASES_DECLINED"
    CASES_DEFERRED = "CASES_DEFERRED"
    CASES_DISPOSED = "CASES_DISPOSED"
    CASES_FILED = "CASES_FILED"
    CASES_OVERTURNED_ON_APPEAL = "CASES_OVERTURNED_ON_APPEAL"
    CASES_PROSECUTED = "CASES_PROSECUTED"
    CASES_REFERRED = "CASES REFERRED"
    COMPLAINTS_SUSTAINED = "COMPLAINTS_SUSTAINED"
    GRIEVANCES_UPHELD = "GRIEVANCES_UPHELD"
    POPULATION = "POPULATION"
    PRETRIAL_RELEASES = "PRETRIAL_RELEASES"
    READMISSIONS = "READMISSIONS"
    RECONVICTIONS = "RECONVICTIONS"
    RELEASES = "RELEASES"
    REPORTED_CRIME = "REPORTED_CRIME"
    RESIDENTS = "RESIDENTS"
    REVOCATIONS = "REVOCATIONS"
    SENTENCES = "SENTENCES"
    SUPERVISION_STARTS = "SUPERVISION_STARTS"
    SUPERVISION_TERMINATIONS = "SUPERVISION_TERMINATIONS"
    SUPERVISION_VIOLATIONS = "SUPERVISION_VIOLATIONS"
    TOTAL_STAFF = "TOTAL_STAFF"
    USE_OF_FORCE_INCIDENTS = "USE_OF_FORCE_INCIDENTS"
    VIOLATIONS_WITH_DISCIPLINARY_ACTION = "VIOLATIONS_WITH_DISCIPLINARY_ACTION"

    @classmethod
    def metric_type_to_unit(cls) -> Dict[str, str]:
        """Provides a mapping from metric type to unit. Not all metric types need to be included
        in this dictionary because the type itself represent the unit (e.g ADMISSIONS)"""

        return {
            "BUDGET": "USD",
            "REPORTED_CRIME": "REPORTED CRIMES",
            "TOTAL_STAFF": "PEOPLE",
            "POPULATION": "PEOPLE",
            "CALLS_FOR_SERVICE": "CALLS",
        }

    @property
    def unit(self) -> str:
        mapping = MetricType.metric_type_to_unit()
        unit = mapping.get(self.value) or self.value
        # Replace underscore with spaces, if they exist. COMPLAINTS_SUSTAINED -> COMPLAINTS SUSTAINED
        unit = re.sub("_", " ", unit)
        return unit


class MeasurementType(enum.Enum):
    """How the metric over a given time window was reduced to a single point.

    This is not comprehensive and can grow as needed."""

    # Measurement at a single point in time.
    INSTANT = "INSTANT"

    # Measurement averaged over the window.
    AVERAGE = "AVERAGE"

    # Count of events that occurred over the window.
    DELTA = "DELTA"

    # Count of events that occurred over the window, deduplicated based on the person associated with the event such
    # that only a single event for each person is counted. In the future if other deduplication-based counting
    # methodologies are discovered this can be expanded or new fields can be added to cover those.
    PERSON_BASED_DELTA = "PERSON_BASED_DELTA"


class ReportingFrequency(enum.Enum):
    ANNUAL = "ANNUAL"
    MONTHLY = "MONTHLY"


class System(enum.Enum):
    """Part of the overall criminal justice system that this pertains to, as defined by the Justice Counts Framework."""

    DEFENSE = "DEFENSE"
    JAILS = "JAILS"
    LAW_ENFORCEMENT = "LAW_ENFORCEMENT"
    COURT_PROCESSES = "COURT_PROCESSES"
    COURTS_AND_PRETRIAL = "COURTS_AND_PRETRIAL"
    COMMUNITY_SUPERVISION_AND_REENTRY = "COMMUNITY_SUPERVISION_AND_REENTRY"
    CORRECTIONS = "CORRECTIONS"
    PRISONS = "PRISONS"
    PROSECUTION = "PROSECUTION"
    SUPERVISION = "SUPERVISION"


class Project(enum.Enum):
    """Internal projects that ingest Justice Counts data."""

    JUSTICE_COUNTS_DATA_SCAN = "JUSTICE_COUNTS_DATA_SCAN"
    JUSTICE_COUNTS_CONTROL_PANEL = "JUSTICE_COUNTS_CONTROL_PANEL"


class ReportStatus(enum.Enum):
    """Reports can be in one of the following states:
    - NOT STARTED: Report was created, but no data has been filled in
    - DRAFT: Some data has been filled in, but report has not been published and report's
        data is not accessible via the Publisher API or dashboards
    - PUBLISHED: All required data has been filled in and report's data is accessible via
        the Publisher API and dasboards.
    """

    NOT_STARTED = "NOT_STARTED"
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"


class Source(JusticeCountsBase):
    """A website or organization that publishes reports.

    It is not necessarily specific to a state or jurisdiction, but frequently is.
    """

    __tablename__ = "source"

    id = Column(Integer, autoincrement=True)

    name = Column(String(255), nullable=False)

    # Type is the "discriminator" column, and is configured to act as such by the
    # "mapper.polymorphic_on" parameter (see below).
    # This column will store a value which indicates the type of object represented within the row;
    # in this case, either "source" or "agency".
    type = Column(String(255))

    __table_args__ = tuple([PrimaryKeyConstraint(id), UniqueConstraint(name)])

    # We use SQLAlchemy's single table inheritance (https://docs.sqlalchemy.org/en/14/orm/inheritance.html)
    # to represent different types of Sources.
    __mapper_args__ = {"polymorphic_identity": "source", "polymorphic_on": type}


class Agency(Source):
    """A organization that has users and publishes reports via the Control Panel.

    All Agencies are Sources, but not all Sources are Agencies.
    """

    system = Column(Enum(System))
    state_code = Column(String(255))
    fips_county_code = Column(String(255))

    @validates("state_code")
    def validate_state_code(self, _: Any, state_code: str) -> str:
        if not StateCode.is_valid(state_code):
            raise ValueError("Agency state_code is not valid")
        return state_code

    @validates("fips_county_code")
    def validate_fips_county_code(self, _: Any, fips_county_code: str) -> str:
        # fips.validate_country_code raises a Value Error if the county_code is invalid.
        fips.validate_county_code(fips_county_code)
        return fips_county_code

    __mapper_args__ = {
        "polymorphic_identity": "agency",
    }

    def to_json(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "system": self.system.value if self.system else None,
            "state_code": self.state_code,
            "fips_county_code": self.fips_county_code,
        }


class UserAccount(JusticeCountsBase):
    """A user (belonging to one or multiple Agencies) who publishes reports via the Control Panel."""

    __tablename__ = "user_account"

    id = Column(Integer, autoincrement=True)

    # Name that will be displayed in the Control Panel
    # If null, we can show email address instead
    name = Column(String(255), nullable=True)

    # Auth0 is an authentication and authorization platform we use for users of the Control Panel.
    # This field refers to the Auth0 `user_id` property:
    # https://auth0.com/docs/manage-users/user-accounts/identify-users
    # Nullable so that we can create users manually first, and fill in their Auth0 id
    # after they sign in for the first time
    auth0_user_id = Column(String(255), nullable=True)

    # Should be the same email address that the user signed with in via Auth0
    # Used to match up users added manually with users who later sign in via Auth0
    email_address = Column(String(255), nullable=False)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(auth0_user_id),
            UniqueConstraint(email_address, name="unique_email_address"),
        ]
    )

    def to_json(
        self,
        permissions: Optional[List[str]] = None,
        agencies: Optional[List[Agency]] = None,
    ) -> Dict[str, Any]:
        return {
            "id": self.id,
            "auth0_user_id": self.auth0_user_id,
            "email_address": self.email_address,
            "name": self.name,
            "agencies": [agency.to_json() for agency in agencies or []],
            "permissions": permissions or [],
        }

    def name_or_email(self) -> str:
        return self.name or self.email_address


class Report(JusticeCountsBase):
    """A document that is published by a source that contains data pertaining to the Justice Counts Framework."""

    __tablename__ = "report"

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

    # The date the report was created
    created_at = Column(Date)
    # The date the report was published.
    publish_date = Column(Date)
    # The URL for the report on the source's website
    url = Column(String(255))
    # The method used to acquire the data (e.g. scraped).
    acquisition_method = Column(Enum(AcquisitionMethod), nullable=False)
    # If manually entered or collected, the person who collected the data.
    acquired_by = Column(String(255))
    # Project that was responsible for ingesting this data (e.g. data scan, control panel).
    project = Column(Enum(Project), nullable=False)
    # Indicates whether the report is started, drafted, or published
    status = Column(Enum(ReportStatus), nullable=False)
    # If date_range_start and date_range_end are specified,
    # we will enforce that all tables in the report are for the same time period.
    # If they are null, then the tables can be for varying time periods.
    date_range_start = Column(Date)
    date_range_end = Column(Date)  # exclusive
    # Timestamp of last modification (in UTC)
    last_modified_at = Column(DateTime)
    # List of ids of users who have modified the report
    modified_by = Column(ARRAY(Integer))
    # Whether or not the report is "recurring" or not. A recurring report creates a new report on a periodic basis.
    is_recurring = Column(Boolean)
    # If a report is created from a recurring report, this refers to the recurring report that created it.
    recurring_report_id = Column(Integer)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(source_id, type, instance),
            ForeignKeyConstraint([source_id], [Source.id]),
            ForeignKeyConstraint([recurring_report_id], ["report.id"]),
        ]
    )

    source = relationship(Source)
    recurring_report = relationship(
        "Report", uselist=False, remote_side=[id], backref=backref("children")
    )

    report_table_instances = relationship(
        "ReportTableInstance",
        back_populates="report",
        cascade="all, delete",
        lazy="selectin",
        passive_deletes=True,
    )

    datapoints = relationship(
        "Datapoint",
        back_populates="report",
        cascade="all, delete",
        lazy="selectin",
        passive_deletes=True,
    )

    def get_reporting_frequency(self) -> ReportingFrequency:
        if (
            self.date_range_end.year == self.date_range_start.year
            and self.date_range_end.month - self.date_range_start.month == 1
        ):
            inferred_frequency = ReportingFrequency.MONTHLY
        else:
            inferred_frequency = ReportingFrequency.ANNUAL
        report_type_string = str(self.type)
        if report_type_string.strip() != str(inferred_frequency.value):
            raise ValueError(
                f"Invalid Report Type: Report type is {report_type_string}, but inferred a reporting frequency of {str(inferred_frequency.value)} from the report date range."
            )
        return inferred_frequency


class ReportTableDefinition(JusticeCountsBase):
    """The definition for what a table within a report describes."""

    __tablename__ = "report_table_definition"

    id = Column(Integer, autoincrement=True)

    system = Column(Enum(System), nullable=False)
    metric_type = Column(Enum(MetricType), nullable=False)
    measurement_type = Column(Enum(MeasurementType), nullable=False)

    # Any dimensions where the data in the table only accounts for a subset of values for that dimension. For instance,
    # a table for the population metric may only cover data for the prison population, not those on parole or
    # probation. In that case filters would include a filter on population type.
    filtered_dimensions = Column(ARRAY(String(255)), nullable=False)
    # The value for each dimension from the above array.
    filtered_dimension_values = Column(ARRAY(String(255)), nullable=False)
    # The dimensions that the metric is broken down by in the report table. Each cell in a table instance has a unique
    # combination of values for the aggregated dimensions. Dimensions are sorted deterministically within the array.
    aggregated_dimensions = Column(ARRAY(String(255)), nullable=False)

    # Either the label of the table within the report (if the source of the report is the data scan),
    # or the unique key of the corresponding MetricDefinition (if the source of the report is the
    # Control Panel)
    label = Column(String(255), nullable=False)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(
                system,
                metric_type,
                measurement_type,
                filtered_dimensions,
                filtered_dimension_values,
                aggregated_dimensions,
                label,
            ),
        ]
    )


class Datapoint(JusticeCountsBase):
    """A single data point reported by an Agency. This table is essentially
    a collapsed version of ReportTableInstance + Cell.
    """

    __tablename__ = "datapoint"

    id = Column(Integer, autoincrement=True)

    report_id = Column(Integer, nullable=False)
    metric_definition_key = Column(String, nullable=False)

    # Datapoints that are not contexts will not have a context key.
    context_key = Column(String, nullable=True)

    # Value for contexts can be numbers, booleans, or text.
    value_type = Column(Enum(ValueType), nullable=True)

    # The window of time that this data point is measured over,
    # represented by a start date (inclusive) and an end date (exclusive).
    # The data could represent an instant measurement, where the start and end are equal,
    # or a window, e.g. ADP over the last month.
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)

    # Maps dimension identifier to dimension enum value
    # (e.g {"global/gender/restricted": "FEMALE"})
    dimension_identifier_to_member = Column(JSONB, nullable=True)

    # Numeric value of this datapoint. All non-null values are saved as strings.
    # datapoints that represent unreported metric values will have a value of None.
    value = Column(String, nullable=True)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(
                report_id,
                start_date,
                end_date,
                dimension_identifier_to_member,
                context_key,
                metric_definition_key,
            ),
            ForeignKeyConstraint([report_id], [Report.id], ondelete="CASCADE"),
        ]
    )

    report = relationship(Report, back_populates="datapoints")
    datapoint_histories = relationship(
        "DatapointHistory",
        back_populates="datapoint",
        cascade="all, delete",
        lazy="selectin",
        passive_deletes=True,
    )

    def get_value(self) -> Any:
        value = self.value
        if value is None:
            return value
        if self.context_key is None or self.value_type == ValueType.NUMBER:
            value = int(value)
        elif self.value_type == ValueType.BOOLEAN:
            value = value == "True"
        return value


class ReportTableInstance(JusticeCountsBase):
    """An instance of a table that contains an actual set of data points along a shared set of dimensions.

    It typically maps to a literal table with columns and rows in a report, but in some cases a literal table in the
    report may map to multiple tables as defined here.
    """

    __tablename__ = "report_table_instance"

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
    methodology = Column(String)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            # TODO(#4476): We need to include time window as part of the unique
            # constraint in case there is data for the same table definition that
            # represents multiple time windows within a single report. To make this
            # work with updates, I think we will re-ingest all table instances for
            # a particular report table definition in an updated report.
            UniqueConstraint(
                report_id,
                report_table_definition_id,
                time_window_start,
                time_window_end,
            ),
            ForeignKeyConstraint([report_id], [Report.id], ondelete="CASCADE"),
            ForeignKeyConstraint(
                [report_table_definition_id], [ReportTableDefinition.id]
            ),
        ]
    )

    report = relationship(Report, back_populates="report_table_instances")
    report_table_definition = relationship(ReportTableDefinition)
    cells = relationship(
        "Cell",
        back_populates="report_table_instance",
        lazy="selectin",
        cascade="all, delete",
        passive_deletes=True,
    )


class Cell(JusticeCountsBase):
    """A single data point within a table."""

    __tablename__ = "cell"

    id = Column(Integer, autoincrement=True)

    report_table_instance_id = Column(Integer, nullable=False)
    aggregated_dimension_values = Column(ARRAY(String(255)), nullable=False)

    value = Column(Numeric, nullable=False)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(report_table_instance_id, aggregated_dimension_values),
            ForeignKeyConstraint(
                [report_table_instance_id], [ReportTableInstance.id], ondelete="CASCADE"
            ),
        ]
    )

    report_table_instance = relationship(ReportTableInstance, back_populates="cells")


class DatapointHistory(JusticeCountsBase):
    """Represents a history of changes made to a Datapoint."""

    __tablename__ = "datapoint_history"

    id = Column(Integer, autoincrement=True)

    datapoint_id = Column(Integer, nullable=False)
    user_account_id = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False)
    old_value = Column(String, nullable=True)
    new_value = Column(String, nullable=True)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            ForeignKeyConstraint([datapoint_id], [Datapoint.id], ondelete="CASCADE"),
            ForeignKeyConstraint([user_account_id], [UserAccount.id]),
        ]
    )

    datapoint = relationship(Datapoint, back_populates="datapoint_histories")


# As this is a TypeVar, it should be used when all variables within the scope of this type should have the same
# concrete class.
JusticeCountsDatabaseEntity = TypeVar(
    "JusticeCountsDatabaseEntity", bound=JusticeCountsBase
)
