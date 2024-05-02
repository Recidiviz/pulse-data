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
from typing import Any, Dict, List, Optional, Set, TypeVar

from sqlalchemy import BOOLEAN, ForeignKey
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
    TIMESTAMP,
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


class UserAccountInvitationStatus(enum.Enum):
    NOT_SENT = "NOT_SENT"
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    ERRORED = "ERRORED"


class UserAccountRole(enum.Enum):
    AGENCY_ADMIN = "AGENCY_ADMIN"
    JUSTICE_COUNTS_ADMIN = "JUSTICE_COUNTS_ADMIN"
    CONTRIBUTOR = "CONTRIBUTOR"
    READ_ONLY = "READ_ONLY"


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
    CASELOADS_PEOPLE = "CASELOADS_PEOPLE"
    CASELOADS_STAFF = "CASELOADS_STAFF"
    CASES_APPOINTED_COUNSEL = "CASES_APPOINTED_COUNSEL"
    CASES_DECLINED = "CASES_DECLINED"
    CASES_DEFERRED = "CASES_DEFERRED"
    CASES_DISPOSED = "CASES_DISPOSED"
    CASES_DIVERTED = "CASES_DIVERTED"
    CASES_FILED = "CASES_FILED"
    CASES_OVERTURNED_ON_APPEAL = "CASES_OVERTURNED_ON_APPEAL"
    CASES_PROSECUTED = "CASES_PROSECUTED"
    CASES_REFERRED = "CASES REFERRED"
    COMPLAINTS_SUSTAINED = "COMPLAINTS_SUSTAINED"
    EXPENSES = "EXPENSES"
    FUNDING = "FUNDING"
    GRIEVANCES_UPHELD = "GRIEVANCES_UPHELD"
    POPULATION = "POPULATION"
    PRETRIAL_RELEASES = "PRETRIAL_RELEASES"
    PRE_ADJUDICATION_ADMISSIONS = "PRE_ADJUDICATION_ADMISSIONS"
    POST_ADJUDICATION_ADMISSIONS = "POST_ADJUDICATION_ADMISSIONS"
    PRE_ADJUDICATION_POPULATION = "PRE_ADJUDICATION_POPULATION"
    POST_ADJUDICATION_POPULATION = "POST_ADJUDICATION_POPULATION"
    PRE_ADJUDICATION_RELEASES = "PRE_ADJUDICATION_RELEASES"
    POST_ADJUDICATION_RELEASES = "POST_ADJUDICATION_RELEASES"
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
        in this dictionary because the type itself represent the unit (e.g ADMISSIONS)
        """

        return {
            "BUDGET": "USD",
            "FUNDING": "USD",
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

    # Used by control panel
    SUPERAGENCY = "SUPERAGENCY"
    LAW_ENFORCEMENT = "LAW_ENFORCEMENT"
    PROSECUTION = "PROSECUTION"
    DEFENSE = "DEFENSE"
    COURTS_AND_PRETRIAL = "COURTS_AND_PRETRIAL"
    JAILS = "JAILS"
    PRISONS = "PRISONS"
    SUPERVISION = "SUPERVISION"
    PAROLE = "PAROLE"
    PROBATION = "PROBATION"
    PRETRIAL_SUPERVISION = "PRETRIAL_SUPERVISION"
    OTHER_SUPERVISION = "OTHER_SUPERVISION"

    # Used by state scan
    CORRECTIONS = "CORRECTIONS"

    # Unused
    COURT_PROCESSES = "COURT_PROCESSES"
    COMMUNITY_SUPERVISION_AND_REENTRY = "COMMUNITY_SUPERVISION_AND_REENTRY"
    POST_RELEASE = "POST_RELEASE"
    DUAL_SUPERVISION = "DUAL_SUPERVISION"

    @classmethod
    def sort(cls, systems: List["System"]) -> List["System"]:
        # Sort a list of systems according to the order in which they are
        # defined in the enum (rationale being mainly so that Supervision,
        # Parole, Probation, and Post-Release are always grouped together,
        # in that order)
        all_systems_ordered: List["System"] = list(cls)
        return sorted(list(systems), key=all_systems_ordered.index)

    @classmethod
    def supervision_subsystems(cls) -> Set["System"]:
        return {
            cls.PAROLE,
            cls.PROBATION,
            cls.PRETRIAL_SUPERVISION,
            cls.OTHER_SUPERVISION,
        }


class Project(enum.Enum):
    """Internal projects that ingest Justice Counts data."""

    JUSTICE_COUNTS_DATA_SCAN = "JUSTICE_COUNTS_DATA_SCAN"
    JUSTICE_COUNTS_CONTROL_PANEL = "JUSTICE_COUNTS_CONTROL_PANEL"


class SpreadsheetStatus(enum.Enum):
    """Spreadsheets can be in one of the following states:
    - UP: The spreadsheet has been uploaded by a user.
    - INGESTED: A recidiviz admin has ingested the spreadsheet and the data has been recorded.
    - ERRORED: The spreadsheet ingest has raised an error, and as a result, the spreadsheet has not been
      successfully processed and spreadsheet data has not been recorded.
    """

    UPLOADED = "UPLOADED"
    INGESTED = "INGESTED"
    ERRORED = "ERRORED"


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


class AgencyUserAccountAssociation(JusticeCountsBase):
    """This table maintains the many-to-many relationship between UserAccount and Agency."""

    __tablename__ = "agency_user_account_association"

    agency_id = Column(ForeignKey("source.id"), primary_key=True)
    user_account_id = Column(ForeignKey("user_account.id"), primary_key=True)
    invitation_status = Column(Enum(UserAccountInvitationStatus), nullable=True)
    role = Column(Enum(UserAccountRole), nullable=True)
    # Tracks the date of the user's last visit to the agency's page
    last_visit = Column(TIMESTAMP(timezone=True), nullable=True)

    # Tracks progress during guidance/onboarding flow
    guidance_progress = Column(JSONB, nullable=True)

    # A boolean to indicate whether or not the user is subscribed to email confirmations
    # for Automated Bulk Upload feature. Emails are sent via SendGrid, and users can
    # opt-out (unsubscribe) via SenGrid unsubscribe link within emails
    subscribed = Column(Boolean, nullable=True, default=False)

    agency = relationship("Agency", back_populates="user_account_assocs")

    # Integer representing the number of days after a completed reporting period
    # that a user wants to receive an email notifying them that they are missing
    # metrics from that report.
    days_after_time_period_to_send_email = Column(Integer, nullable=True)

    user_account = relationship("UserAccount", back_populates="agency_assocs")


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

    # Indicates whether or not this agency is a "Superagency" (i.e. an agency that
    # has permission to upload data for several child agencies)
    is_superagency = Column(Boolean)

    # If this agency is a child agency (i.e. belongs to a parent Superagency)
    # then this column is populated with the ID of that parent agency.
    super_agency_id = Column(Integer, nullable=True)

    # If the agency has a public dashboard is_dashboard_enabled will be True.
    # If is_dashboard_enabled is None or False, their data will only be accessible
    # internally.
    is_dashboard_enabled = Column(Boolean, nullable=True)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            ForeignKeyConstraint(
                [super_agency_id], ["source.id"], name="super_agency_id_constraint"
            ),
        ]
    )

    # We use SQLAlchemy's single table inheritance (https://docs.sqlalchemy.org/en/14/orm/inheritance.html)
    # to represent different types of Sources.
    __mapper_args__ = {"polymorphic_identity": "source", "polymorphic_on": type}


class Agency(Source):
    """A organization that has users and publishes reports via the Control Panel.

    All Agencies are Sources, but not all Sources are Agencies.
    """

    # BigQuery doesn't like arrays of Enums, so we just use strings
    systems = Column(ARRAY(String(255)))
    state_code = Column(String(255))
    fips_county_code = Column(String(255))
    # The date in which the Agency was created in our platform
    created_at = Column(TIMESTAMP(timezone=True), nullable=True)

    # Custom name for the agency, which is a child agency, that
    # will be used during Bulk Upload
    custom_child_agency_name = Column(String(255), nullable=True)

    @validates("state_code")
    def validate_state_code(self, _: Any, state_code: str) -> str:
        if not StateCode.is_valid(state_code):
            raise ValueError("Agency state_code is not valid")
        return state_code

    @validates("fips_county_code")
    def validate_fips_county_code(
        self, _: Any, fips_county_code: Optional[str]
    ) -> Optional[str]:
        if fips_county_code is None:
            return None

        # fips.validate_country_code raises a Value Error if the county_code is invalid.
        fips.validate_county_code(fips_county_code)
        return fips_county_code

    def get_state_name(self) -> Optional[str]:
        state_name = None
        if self.state_code is not None:
            state_name = StateCode(self.state_code.upper()).get_state().name
        return state_name

    user_account_assocs = relationship(
        "AgencyUserAccountAssociation", back_populates="agency"
    )

    agency_settings = relationship("AgencySetting")

    __mapper_args__ = {
        "polymorphic_identity": "agency",
    }

    def to_json_simple(self) -> Dict[str, Any]:
        """Used when only basic information about an Agency is needed,
        e.g. when we just need to know the names of a superagency's
        child agencies.
        """
        return {
            "id": self.id,
            "name": self.name,
            "custom_child_agency_name": self.custom_child_agency_name,
            "systems": [
                system_enum.value
                for system_enum in System.sort(
                    systems=[System(system_str) for system_str in (self.systems or [])]
                )
            ],
        }

    def to_json(
        self, with_team: bool = True, with_settings: bool = True
    ) -> Dict[str, Any]:
        """Used when more complete information about an agency is needed."""
        return {
            **self.to_json_simple(),
            **{
                "state_code": self.state_code,
                "fips_county_code": self.fips_county_code,
                "state": self.get_state_name(),
                "team": (
                    [
                        {
                            "name": assoc.user_account.name,
                            "user_account_id": assoc.user_account_id,
                            "auth0_user_id": assoc.user_account.auth0_user_id,
                            "email": assoc.user_account.email,
                            "invitation_status": (
                                assoc.invitation_status.value
                                if assoc.invitation_status is not None
                                else None
                            ),
                            "role": (
                                assoc.role.value if assoc.role is not None else None
                            ),
                        }
                        for assoc in self.user_account_assocs
                    ]
                    if with_team is True
                    else []
                ),
                "settings": (
                    [setting.to_json() for setting in self.agency_settings]
                    if with_settings is True
                    else []
                ),
                "is_superagency": self.is_superagency,
                "super_agency_id": self.super_agency_id,
                "is_dashboard_enabled": self.is_dashboard_enabled,
                "created_at": self.created_at,
            },
        }

    def to_public_json(self) -> Dict[str, Any]:
        whitelisted_agency_settings = list(
            filter(
                lambda setting: setting.setting_type
                in [
                    AgencySettingType.PURPOSE_AND_FUNCTIONS.value,
                    AgencySettingType.HOMEPAGE_URL.value,
                ],
                self.agency_settings,
            )
        )
        return {
            "id": self.id,
            "name": self.name,
            "is_dashboard_enabled": self.is_dashboard_enabled,
            "systems": [
                system_enum.value
                for system_enum in System.sort(
                    systems=[System(system_str) for system_str in (self.systems or [])]
                )
            ],
            "settings": [setting.to_json() for setting in whitelisted_agency_settings],
        }


class UserAccount(JusticeCountsBase):
    """A user (belonging to one or multiple Agencies) who publishes reports via the Publisher.
    This table should only be used when necessary as a cache for certain features."""

    __tablename__ = "user_account"

    id = Column(Integer, autoincrement=True)

    # Name that will be displayed in the Control Panel
    name = Column(String(255), nullable=True)

    # name, invitation_status, and role will be displayed
    # in the team management tool within agency settings.
    email = Column(String(255), nullable=True)

    # Auth0 is an authentication and authorization platform we use for users of the Control Panel.
    # This field refers to the Auth0 `user_id` property:
    # https://auth0.com/docs/manage-users/user-accounts/identify-users
    # Nullable so that we can create users manually first, and fill in their Auth0 id
    # after they sign in for the first time
    auth0_user_id = Column(String(255), nullable=True)

    # (DEPRECATED - moved to AgencyUserAccountAssociation table) Tracks progress during guidance/onboarding flow
    guidance_progress = Column(JSONB, nullable=True)

    agency_assocs = relationship(
        "AgencyUserAccountAssociation",
        back_populates="user_account",
        lazy="selectin",  # by default, always load agency associations with the user
    )

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(auth0_user_id, name="unique_auth0_user_id"),
        ]
    )

    def to_json(
        self,
        agencies: Optional[List[Agency]] = None,
    ) -> Dict[str, Any]:
        return {
            "id": self.id,
            "auth0_user_id": self.auth0_user_id,
            "name": self.name,
            "email": self.email,
            "agencies": [
                agency.to_json(with_team=False, with_settings=False)
                for agency in agencies or []
            ],
        }


class Spreadsheet(JusticeCountsBase):
    """A single spreadsheet uploaded by an Agency."""

    __tablename__ = "spreadsheet"

    id = Column(Integer, autoincrement=True)
    # The original filename of the spreadsheet (e.g "justice_counts_metrics.xlsx")
    original_name = Column(String, nullable=False)
    # The standardized filename of the spreadsheet, formatted as "<agency_id>:<system>:<upload_timestamp>.xlsx"
    standardized_name = Column(String, nullable=False)
    agency_id = Column(Integer, nullable=False)
    system = Column(Enum(System), nullable=False)
    status = Column(Enum(SpreadsheetStatus), nullable=False)
    # The date the spreadsheet was uploaded
    uploaded_at = Column(DateTime, nullable=False)
    # The auth0_user_id of the user who uploaded the spreadsheet
    uploaded_by = Column(String, nullable=True)
    # The auth0_user_id of the user who ingested the spreadsheet
    ingested_by = Column(String, nullable=True)
    # The date the spreadsheet was ingested
    ingested_at = Column(DateTime, nullable=True)

    upload_note = Column(String, nullable=True)

    __table_args__ = tuple(
        [PrimaryKeyConstraint(id), ForeignKeyConstraint([agency_id], [Agency.id])]
    )


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
    # List of ids of users who have modified the report.
    # Some user_ids here might not map to a user stored in the database since we do not
    # remove users from this array upon deletion of a user.
    modified_by = Column(ARRAY(Integer))
    # Whether or not the report is "recurring" or not. A recurring report creates a new report on a periodic basis.
    is_recurring = Column(Boolean)
    # If a report is created from a recurring report, this refers to the recurring report that created it.
    recurring_report_id = Column(Integer)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(
                source_id,
                type,
                instance,
                date_range_start,
                date_range_end,
                name="unique_report",
            ),
            ForeignKeyConstraint([source_id], [Source.id]),
            ForeignKeyConstraint([recurring_report_id], ["report.id"]),
        ]
    )

    source = relationship(Source)

    # All Agencies are Sources, but not all Sources are agencies
    # report.source will only load the Source columns, not the Agency columns
    # So we need a separate relationship for reports that belong to Agencies
    agency = relationship(Agency, overlaps="source")

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
        lazy="selectin",  # by default, always load datapoints with the report
        passive_deletes=True,
    )


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
                name="report_table_definition_columns_for_key",
            ),
        ]
    )


class Datapoint(JusticeCountsBase):
    """A single data point reported by an Agency. This table is essentially
    a collapsed version of ReportTableInstance + Cell.
    """

    __tablename__ = "datapoint"

    id = Column(Integer, autoincrement=True)

    # Report datapoints will have a non-null report_id.
    report_id = Column(Integer, nullable=True)

    # Agency datapoints will have a non-null source_id.
    source_id = Column(Integer, nullable=True, index=True)

    # Indicates if a datapoint is a Report datapoint (True) or an Agency datapoint (False)
    is_report_datapoint = Column(Boolean, index=True)

    metric_definition_key = Column(String, nullable=False)

    # Datapoints that are not contexts will not have a context key.
    context_key = Column(String, nullable=True)

    # Date created
    created_at = Column(DateTime, nullable=True)

    # Date of the most recent update to the datapoint
    last_updated = Column(DateTime, nullable=True)

    # Agency datapoints that describe what data is being included/excluded
    # in the metric definition will have an includes_excludes_key.
    includes_excludes_key = Column(String, nullable=True)

    # Value for contexts can be numbers, booleans, or text.
    value_type = Column(Enum(ValueType), nullable=True)

    # The window of time that this data point is measured over,
    # represented by a start date (inclusive) and an end date (exclusive).
    # The data could represent an instant measurement, where the start and end are equal,
    # or a window, e.g. ADP over the last month.
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)

    # Maps dimension identifier to dimension enum value
    # (e.g {"global/gender/restricted": "FEMALE"})
    dimension_identifier_to_member = Column(JSONB, nullable=True)

    # Numeric value of this datapoint. All non-null values are saved as strings.
    # datapoints that represent unreported metric values will have a value of None.
    value = Column(String, nullable=True)

    # Agency datapoints will have a nullable enabled value.
    # Disabled metrics will have a value of False.
    # Enabled metrics will have a value of True.
    # Metrics that have not been set/touched by the agency will have a value of None (default).
    enabled = Column(BOOLEAN, nullable=True)

    upload_method = Column(String, nullable=True)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(
                report_id,
                start_date,
                end_date,
                dimension_identifier_to_member,
                context_key,
                includes_excludes_key,
                metric_definition_key,
                source_id,
                name="unique_datapoint",
            ),
            ForeignKeyConstraint([report_id], [Report.id], ondelete="CASCADE"),
            ForeignKeyConstraint(
                [source_id], [Source.id], name="source_foreign_key_constraint"
            ),
        ]
    )

    report = relationship(Report, back_populates="datapoints")
    source = relationship(Source)
    datapoint_histories = relationship(
        "DatapointHistory",
        back_populates="datapoint",
        cascade="all, delete",
        lazy="select",
        passive_deletes=True,
    )


class MetricSetting(JusticeCountsBase):
    """Stores a metric interface for an agency and metric_definition_key combination."""

    __tablename__ = "metric_setting"

    id = Column(Integer, autoincrement=True)

    # The agency that this metric setting is published by.
    agency_id = Column(Integer, nullable=False, index=True)

    # The key of the metric (i.e. `MetricDefinition.key`) that is reported.
    metric_definition_key = Column(String, nullable=False)

    # A json representation of a MetricInterface object. We strip the metric interface
    # of all report datapoints (both aggregated and disaggregated) before storing it in
    # the database.
    metric_interface = Column(String, nullable=True)

    # Date of the most recent update to the metric setting.
    last_updated = Column(DateTime, nullable=True)

    # Date created
    created_at = Column(DateTime, nullable=True)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(
                agency_id,
                metric_definition_key,
                name="unique_metric_setting",
            ),
            ForeignKeyConstraint(
                [agency_id], [Agency.id], name="agency_foreign_key_constraint"
            ),
        ]
    )


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
    user_account_id = Column(Integer, nullable=True)
    timestamp = Column(DateTime, nullable=False)
    old_value = Column(String, nullable=True)
    new_value = Column(String, nullable=True)
    # Only populated for reporting datapoints.
    old_upload_method = Column(String, nullable=True)
    new_upload_method = Column(String, nullable=True)
    # Only populated for agency datapoints for metric/breakdown enabling.
    old_enabled = Column(Boolean, nullable=True)
    new_enabled = Column(Boolean, nullable=True)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            ForeignKeyConstraint([datapoint_id], [Datapoint.id], ondelete="CASCADE"),
            ForeignKeyConstraint([user_account_id], [UserAccount.id]),
        ]
    )

    datapoint = relationship(Datapoint, back_populates="datapoint_histories")


class AgencySettingType(enum.Enum):
    TEST = "TEST"
    PURPOSE_AND_FUNCTIONS = "PURPOSE_AND_FUNCTIONS"
    HOMEPAGE_URL = "HOMEPAGE_URL"


class AgencySetting(JusticeCountsBase):
    """A custom setting for an Agency."""

    __tablename__ = "agency_setting"

    id = Column(Integer, autoincrement=True)

    source_id = Column(Integer, nullable=False)

    # Describes what type of setting it is
    setting_type = Column(String, nullable=False)

    # Value of the setting
    value = Column(JSONB, nullable=True)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            UniqueConstraint(
                source_id,
                setting_type,
                name="unique_agency_setting",
            ),
            ForeignKeyConstraint(
                [source_id], [Source.id], name="source_foreign_key_constraint"
            ),
        ]
    )

    def to_json(self) -> Dict[str, Any]:
        return {
            "setting_type": self.setting_type,
            "value": self.value,
            "source_id": self.source_id,
        }


class AgencyJurisdictionType(enum.Enum):
    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"


class AgencyJurisdiction(JusticeCountsBase):
    """A table of Jurisdictions that are included and excluded for a given Agency"""

    __tablename__ = "agency_jurisdictions"

    id = Column(Integer, autoincrement=True)

    # id of the agency
    source_id = Column(Integer, nullable=False)

    # membership (the jurisdiction is either INCLUDE(d) or EXCLUDE(d) from an agency)
    membership = Column(String, nullable=False)

    # jurisdiction_id (the unique id that identifies a given jurisdiction)
    jurisdiction_id = Column(String, nullable=False)

    __table_args__ = tuple(
        [
            PrimaryKeyConstraint(id),
            ForeignKeyConstraint(
                [source_id], [Source.id], name="source_foreign_key_constraint"
            ),
        ]
    )

    source = relationship(Source)


# As this is a TypeVar, it should be used when all variables within the scope of this type should have the same
# concrete class.
JusticeCountsDatabaseEntity = TypeVar(
    "JusticeCountsDatabaseEntity", bound=JusticeCountsBase
)
