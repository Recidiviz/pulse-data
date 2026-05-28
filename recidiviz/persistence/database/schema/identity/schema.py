# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Define the ORM schema objects for the Identity Service.

See the Identity System Design v3 doc for the data model background. The
classes here cover three groups of tables: identity records (with attribute
children), candidates flagged for human review by the import DAG, and audit
trails of merges and splits.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
)
from sqlalchemy import Identity as SaIdentity
from sqlalchemy import Index, MetaData, PrimaryKeyConstraint, String, sql
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeMeta, declarative_base

from recidiviz.common.constants.identity import (
    AttributeType,
    CandidateStatus,
    CreateCandidateStatus,
    IdentifierType,
    IdentityStatus,
    MergeTrigger,
    NameUse,
    PersonType,
    PhoneType,
    ProductApp,
    SourceType,
    SplitTrigger,
    Tenant,
)
from recidiviz.common.demographics import Ethnicity as DemographicsEthnicity
from recidiviz.common.demographics import Gender as DemographicsGender
from recidiviz.common.demographics import Race as DemographicsRace
from recidiviz.common.demographics import Sex as DemographicsSex
from recidiviz.persistence.database.column_types import StringBackedEnum
from recidiviz.persistence.database.database_entity import DatabaseEntity

IDENTITY_NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(referred_table_name)s_%(column_0_name)s",
    "pk": "pk_%(table_name)s",
}
IdentityBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity,
    name="IdentityBase",
    metadata=MetaData(naming_convention=IDENTITY_NAMING_CONVENTION),
)

# CHECK that the source_type/source_product_app combination is valid: an
# EXTERNAL_DATA_SYSTEM or ADMIN_OVERRIDE source needs no product app, but a
# PRODUCT_APP source must name one. Applied to every attribute table.
_SOURCE_PROVENANCE_CHECK = (
    "(source_type = 'EXTERNAL_DATA_SYSTEM') "
    "OR (source_type = 'PRODUCT_APP' AND source_product_app IS NOT NULL) "
    "OR (source_type = 'ADMIN_OVERRIDE')"
)


class Identity(IdentityBase):
    """Master identity record, keyed by an immutable Recidiviz-assigned UUID."""

    __tablename__ = "identities"

    __table_args__ = (
        # A RETIRED identity must point at the surviving record via merged_into; an
        # ACTIVE identity must not.
        CheckConstraint(
            "status != 'RETIRED' OR merged_into IS NOT NULL",
            name="retired_requires_merged_into",
        ),
        CheckConstraint(
            "merged_into IS NULL OR status = 'RETIRED'",
            name="merged_into_requires_retired",
        ),
        Index("ix_identities_tenant", "tenant"),
    )

    recidiviz_id = Column(UUID(as_uuid=True), primary_key=True)
    """Immutable Recidiviz-assigned id for this person."""

    created_utc = Column(DateTime, nullable=False)
    """When the identity record was first created."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When any field on this identity or its child attributes was last modified."""

    tenant = Column(StringBackedEnum(Tenant), nullable=False)
    """The jurisdiction or organization through which the person entered the
    system. Usually a state_code (e.g., 'US_PA'), but can also be a city code,
    a federal identifier, or 'RECIDIVIZ' for Recidiviz employees.
    """

    person_type = Column(StringBackedEnum(PersonType), nullable=False)
    """Category of person this identity represents (see PersonType)."""

    status = Column(StringBackedEnum(IdentityStatus), nullable=False)
    """Lifecycle status: ACTIVE for active records, RETIRED after a merge."""

    merged_into = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=True,
    )
    """The surviving identity that absorbed this one. NULL for ACTIVE identities."""

    last_cluster_hash = Column(String, nullable=True)
    """Hash of the cluster's external IDs and attributes as of the last successful
    import run.
    """

    skip_demographic_guard = Column(Boolean, nullable=False, server_default=sql.false())
    """When TRUE, POST /import skips the demographic guard for this identity
    during the per-cluster update pass. Set on the original identity after an
    auto-split so the update pass can correct its canonical EXTERNAL_DATA_SYSTEM
    attributes without the guard blocking. Cleared back to FALSE immediately
    after the successful update pass in the same import run."""


class ExternalId(IdentityBase):
    """External system identifier attached to an identity. Many-to-one."""

    __tablename__ = "external_ids"

    __table_args__ = (
        PrimaryKeyConstraint(
            "external_id",
            "id_type",
            "recidiviz_id",
            name="pk_external_ids",
        ),
        Index("ix_external_ids_recidiviz_id", "recidiviz_id"),
        Index("ix_external_ids_id_type", "id_type"),
        # Up to one (external_id, id_type) pair may be active across all identities
        # within an environment. Inactive rows are residue of splits and can pile up freely.
        Index(
            "uq_external_ids_active",
            "external_id",
            "id_type",
            unique=True,
            postgresql_where=sql.text("is_active = TRUE"),
        ),
    )

    external_id = Column(String, nullable=False)
    """The identifier as it appears in the source system."""

    id_type = Column(StringBackedEnum(IdentifierType), nullable=False)
    """Which kind of external identifier this is (e.g., state's person ID, SSN, etc.)."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The Identity this external ID is attached to."""

    is_active = Column(Boolean, nullable=False, server_default=sql.true())
    """FALSE for external IDs left behind by splits (kept for audit history
    but no longer associated with the identity for lookups)."""


class Name(IdentityBase):
    """Sourced name attribute attached to an identity."""

    __tablename__ = "names"

    __table_args__ = (
        CheckConstraint(_SOURCE_PROVENANCE_CHECK, name="source_provenance"),
        Index("ix_names_recidiviz_id", "recidiviz_id"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity this name belongs to."""

    surname = Column(String, nullable=True)
    """Family/last name, if provided."""

    given_name = Column(String, nullable=True)
    """First/given name, if provided."""

    middle_names = Column(ARRAY(String), nullable=False, server_default="{}")
    """Ordered list of middle names."""

    name_suffix = Column(String, nullable=True)
    """Generational or honorific suffix (e.g., 'Jr.', 'III')."""

    use = Column(StringBackedEnum(NameUse), nullable=True)
    """How the name is used (official, preferred, former, alias)."""

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this name information came from (see SourceType)."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this name row was last modified."""


class DateOfBirth(IdentityBase):
    """Sourced date-of-birth attribute. canonical/canonical_locked select which
    row to display when sources disagree."""

    __tablename__ = "dates_of_birth"

    __table_args__ = (
        CheckConstraint(
            _SOURCE_PROVENANCE_CHECK,
            name="source_provenance",
        ),
        Index("ix_dates_of_birth_recidiviz_id", "recidiviz_id"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity this date of birth belongs to."""

    date = Column(Date, nullable=False)
    """The date of birth value."""

    canonical = Column(Boolean, nullable=False, server_default=sql.false())
    """TRUE if this row is the value the system exposes by default when
    multiple sources disagree. At most one row per identity should be canonical."""

    canonical_locked = Column(Boolean, nullable=False, server_default=sql.false())
    """TRUE if the canonical flag has been manually pinned (typically by an
    admin override) and the reconciliation pass will leave it alone."""

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this DoB information came from (see SourceType)."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this date-of-birth row was last modified."""


class Gender(IdentityBase):
    """Sourced gender attribute. canonical/canonical_locked work as in
    DateOfBirth."""

    __tablename__ = "genders"

    __table_args__ = (
        CheckConstraint(_SOURCE_PROVENANCE_CHECK, name="source_provenance"),
        Index("ix_genders_recidiviz_id", "recidiviz_id"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The Identity this gender belongs to."""

    gender = Column(StringBackedEnum(DemographicsGender), nullable=False)
    """The gender value."""

    canonical = Column(Boolean, nullable=False, server_default=sql.false())
    """TRUE if this row is the value the system exposes by default when
    multiple sources disagree. At most one row per identity should be canonical."""

    canonical_locked = Column(Boolean, nullable=False, server_default=sql.false())
    """When TRUE, the canonical flag has been manually pinned (typically by an
    admin override) and the reconciliation pass will leave it alone."""

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this gender value came from."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this gender row was last modified."""


class Race(IdentityBase):
    """Sourced race attribute."""

    __tablename__ = "races"

    __table_args__ = (
        CheckConstraint(_SOURCE_PROVENANCE_CHECK, name="source_provenance"),
        Index("ix_races_recidiviz_id", "recidiviz_id"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity this race belongs to."""

    race = Column(StringBackedEnum(DemographicsRace), nullable=False)
    """The race value."""

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this race value came from."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this race row was last modified."""


class Sex(IdentityBase):
    """Sourced sex attribute. canonical/canonical_locked work as in DateOfBirth."""

    __tablename__ = "sexes"

    __table_args__ = (
        CheckConstraint(_SOURCE_PROVENANCE_CHECK, name="source_provenance"),
        Index("ix_sexes_recidiviz_id", "recidiviz_id"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity this sex belongs to."""

    sex = Column(StringBackedEnum(DemographicsSex), nullable=False)
    """The sex value."""

    canonical = Column(Boolean, nullable=False, server_default=sql.false())
    """TRUE if this row is the value the system exposes by default when
    multiple sources disagree. At most one row per identity should be canonical."""

    canonical_locked = Column(Boolean, nullable=False, server_default=sql.false())
    """TRUE if the canonical flag has been manually pinned (typically by an
    admin override) and the reconciliation pass will leave it alone."""

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this sex value came from."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this sex row was last modified."""


class Ethnicity(IdentityBase):
    """Sourced ethnicity attribute. canonical/canonical_locked work as in DateOfBirth."""

    __tablename__ = "ethnicities"

    __table_args__ = (
        CheckConstraint(_SOURCE_PROVENANCE_CHECK, name="source_provenance"),
        Index("ix_ethnicities_recidiviz_id", "recidiviz_id"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity this ethnicity belongs to."""

    ethnicity = Column(StringBackedEnum(DemographicsEthnicity), nullable=False)
    """The ethnicity value."""

    canonical = Column(Boolean, nullable=False, server_default=sql.false())
    """TRUE if this row is the value the system exposes by default when
    multiple sources disagree. At most one row per identity should be canonical."""

    canonical_locked = Column(Boolean, nullable=False, server_default=sql.false())
    """TRUE if the canonical flag has been manually pinned (typically by an
    admin override) and the reconciliation pass will leave it alone."""

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this ethnicity value came from."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this ethnicity row was last modified."""


class PhoneNumber(IdentityBase):
    """Sourced phone number attribute."""

    __tablename__ = "phone_numbers"

    __table_args__ = (
        CheckConstraint(
            _SOURCE_PROVENANCE_CHECK,
            name="source_provenance",
        ),
        Index("ix_phone_numbers_recidiviz_id", "recidiviz_id"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity this phone number belongs to."""

    number = Column(String, nullable=False)
    """The phone number, stored as the source provided it (no normalization
    enforced at the schema layer)."""

    type = Column(StringBackedEnum(PhoneType), nullable=True)
    """Category of phone (cell/home/work/other), if provided."""

    preferred = Column(Boolean, nullable=True)
    """TRUE if this is the person's preferred phone."""

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this phone number came from."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this phone number row was last modified."""


class Email(IdentityBase):
    """Sourced email address attribute. address_hash is a normalized hash used
    for the application-level uniqueness check (uniqueness is not enforced at
    the DB layer so the import process can log and skip conflicting emails rather
    than fail)."""

    __tablename__ = "emails"

    __table_args__ = (
        CheckConstraint(_SOURCE_PROVENANCE_CHECK, name="source_provenance"),
        Index("ix_emails_recidiviz_id", "recidiviz_id"),
        Index("ix_emails_address", "address"),
        Index("ix_emails_address_hash", "address_hash"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity this email belongs to."""

    address = Column(String, nullable=False)
    """The email address as the source provided it (case preserved)."""

    address_hash = Column(String, nullable=False)
    """Normalized hash of the email address used for the application-layer
    uniqueness check. See `generate_user_hash()` function in `recidiviz/auth/helpers.py`.
    """

    source_type = Column(StringBackedEnum(SourceType), nullable=False)
    """Where this email came from."""

    source_product_app = Column(StringBackedEnum(ProductApp), nullable=True)
    """When `source_type` is `PRODUCT_APP`, the app that set the value. NULL otherwise."""

    last_updated_utc = Column(DateTime, nullable=False)
    """When this email row was last modified."""


class CreateCandidate(IdentityBase):
    """Cluster flagged as a candidate for identity creation but
    held back for human review."""

    __tablename__ = "create_candidates"

    __table_args__ = (
        Index(
            "uq_create_candidates_pending_cluster_id",
            "cluster_id",
            unique=True,
            postgresql_where=sql.text("status = 'PENDING'"),
        ),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    cluster_id = Column(String, nullable=False)
    """Deterministic identifier for the cluster of incoming records associated
    with this candidate, computed by hashing the cluster's tenant and sorted
    external IDs. Used as the partial-unique key for dedup across import runs,
    but is NOT stable if the cluster gains new external IDs between runs
    (since the hash changes)."""

    tenant = Column(StringBackedEnum(Tenant), nullable=False)
    """Tenant the would-be identity would be created under."""

    cluster_external_ids = Column(JSONB, nullable=False)
    """JSONB snapshot of the (external_id, id_type) pairs that make up the
    cluster being proposed."""

    conflicting_attributes = Column(JSONB, nullable=False)
    """JSONB description of why the cluster was held back — e.g., which
    attributes disagreed."""

    detected_at_utc = Column(DateTime, nullable=False)
    """When the import run flagged this cluster."""

    status = Column(
        StringBackedEnum(CreateCandidateStatus),
        nullable=False,
        server_default="PENDING",
    )
    """PENDING until a reviewer acts; RESOLVED afterwards."""


class UpdateAttributeCandidate(IdentityBase):
    """Existing identity whose POST /import attribute update was held for
    review."""

    __tablename__ = "update_attribute_candidates"

    __table_args__ = (
        Index(
            "uq_update_attribute_candidates_pending_recidiviz_id",
            "recidiviz_id",
            unique=True,
            postgresql_where=sql.text("status = 'PENDING'"),
        ),
        Index(
            "ix_update_attribute_candidates_pending_cleanup",
            "tenant",
            "detected_at_utc",
            postgresql_where=sql.text("status = 'PENDING'"),
        ),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    tenant = Column(StringBackedEnum(Tenant), nullable=False)
    """Tenant whose import run produced this candidate. Denormalized from the
    identity row to allow efficient per-tenant cleanup queries."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity whose attribute update was held."""

    detected_at_utc = Column(DateTime, nullable=False)
    """When the import run flagged this update."""

    incoming_attributes = Column(JSONB, nullable=False)
    """JSONB snapshot of the proposed attribute changes the import wanted to
    apply."""

    status = Column(
        StringBackedEnum(CandidateStatus),
        nullable=False,
        server_default="PENDING",
    )
    """PENDING / RESOLVED / STALE — STALE if intervening writes superseded
    this update."""


class MergeCandidate(IdentityBase):
    """Set of identities a caller wants to merge but that needs human approval."""

    __tablename__ = "merge_candidates"

    __table_args__ = (
        Index(
            "ix_merge_candidates_pending_cleanup",
            "tenant",
            "detected_at_utc",
            postgresql_where=sql.text("status = 'PENDING'"),
        ),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier. The participating identities are listed
    in merge_candidate_identities."""

    tenant = Column(StringBackedEnum(Tenant), nullable=False)
    """Tenant whose import run produced this candidate. Denormalized to allow
    efficient per-tenant cleanup without joining through identities."""

    detected_at_utc = Column(DateTime, nullable=False)
    """When the merge was first proposed."""

    conflicting_attributes = Column(JSONB, nullable=False)
    """JSONB description of the conflicts preventing an automatic merge."""

    status = Column(
        StringBackedEnum(CandidateStatus),
        nullable=False,
        server_default="PENDING",
    )
    """PENDING / RESOLVED / STALE — STALE if intervening writes invalidated
    the proposed merge."""


class MergeCandidateIdentity(IdentityBase):
    """Link table: which identities belong to a given merge_candidate."""

    __tablename__ = "merge_candidate_identities"

    __table_args__ = (
        PrimaryKeyConstraint(
            "merge_candidate_id",
            "recidiviz_id",
            name="pk_merge_candidate_identities",
        ),
    )

    merge_candidate_id = Column(
        BigInteger,
        ForeignKey("merge_candidates.id"),
        nullable=False,
    )
    """The merge_candidate row this identity is a participant in."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """One of the identities proposed for merge."""


class SplitCandidate(IdentityBase):
    """Existing identity flagged for splitting; one row per identity."""

    __tablename__ = "split_candidates"

    __table_args__ = (
        Index(
            "uq_split_candidates_pending_recidiviz_id",
            "recidiviz_id",
            unique=True,
            postgresql_where=sql.text("status = 'PENDING'"),
        ),
        Index(
            "ix_split_candidates_pending_cleanup",
            "tenant",
            "detected_at_utc",
            postgresql_where=sql.text("status = 'PENDING'"),
        ),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    tenant = Column(StringBackedEnum(Tenant), nullable=False)
    """Tenant whose import run produced this candidate. Denormalized to allow
    efficient per-tenant cleanup without joining through identities."""

    recidiviz_id = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """The identity flagged for splitting."""

    cluster_groups = Column(JSONB, nullable=False)
    """JSONB structure describing the groups the split would partition the
    identity's external IDs and attributes into."""

    detected_at_utc = Column(DateTime, nullable=False)
    """When the split was first proposed."""

    status = Column(
        StringBackedEnum(CandidateStatus),
        nullable=False,
        server_default="PENDING",
    )
    """PENDING / RESOLVED / STALE — STALE if intervening writes invalidated
    the proposed split."""


# The audit/event tables below intentionally omit foreign keys on their
# identity-referencing columns (surviving_id, retired_id, original_id,
# new_recidiviz_id). They are an append-only historical log that must survive
# deletion of the identities it describes, so the reference is enforced by the
# application rather than the database.
class MergeEvent(IdentityBase):
    """Audit record of an identity merge."""

    __tablename__ = "merge_events"

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    surviving_id = Column(UUID(as_uuid=True), nullable=False)
    """The identity that remained ACTIVE after the merge."""

    retired_id = Column(UUID(as_uuid=True), nullable=False)
    """The identity that became RETIRED after the merge."""

    trigger = Column(StringBackedEnum(MergeTrigger), nullable=False)
    """What caused the merge (import pipeline or explicit endpoint call)."""

    requested_by = Column(String, nullable=True)
    """For MERGE_ENDPOINT triggers, the email of the user who called the
    endpoint (derived from their Auth0 JWT claims). NULL for IMPORT triggers."""

    timestamp_utc = Column(DateTime, nullable=False)
    """When the merge was performed."""


class AttributeConflict(IdentityBase):
    """Snapshot of a same-source attribute conflict resolved during a merge.

    JSONB snapshots avoid joins to historical attribute rows that may
    themselves have been edited or removed since the merge."""

    __tablename__ = "attribute_conflicts"

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    merge_event_id = Column(BigInteger, ForeignKey("merge_events.id"), nullable=False)
    """The merge_event this conflict was resolved during."""

    attribute_type = Column(StringBackedEnum(AttributeType), nullable=False)
    """Which attribute table the conflict involves (see AttributeType)."""

    retired_value = Column(JSONB, nullable=False)
    """Serialized SourcedAttributeValue from the retired identity at the time
    of the merge — i.e., the typed attribute value plus its source_type,
    source_product_app, and last_updated_utc."""

    surviving_value = Column(JSONB, nullable=False)
    """Serialized SourcedAttributeValue from the surviving identity at the
    time of the merge, in the same shape as retired_value."""


class SplitEvent(IdentityBase):
    """Audit record of an identity split."""

    __tablename__ = "split_events"

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    original_id = Column(UUID(as_uuid=True), nullable=False)
    """The identity that was split. It remains ACTIVE after the split."""

    trigger = Column(StringBackedEnum(SplitTrigger), nullable=False)
    """What caused the split."""

    requested_by = Column(String, nullable=True)
    """For SPLIT_ENDPOINT triggers, the email of the user who called the
    endpoint. NULL otherwise."""

    timestamp_utc = Column(DateTime, nullable=False)
    """When the split was performed."""


class SplitEventNewIdentity(IdentityBase):
    """New identity produced by a split. One row per new identity per
    split_event."""

    __tablename__ = "split_event_new_identities"

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    split_event_id = Column(BigInteger, ForeignKey("split_events.id"), nullable=False)
    """The split_event this new identity was produced by."""

    new_recidiviz_id = Column(UUID(as_uuid=True), nullable=False)
    """The newly-created identity."""


class SplitEventMovedExternalId(IdentityBase):
    """External ID moved off the original identity to one of the new identities
    during a split."""

    __tablename__ = "split_event_moved_external_ids"

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    split_event_id = Column(BigInteger, ForeignKey("split_events.id"), nullable=False)
    """The split_event that moved this external ID."""

    external_id = Column(String, nullable=False)
    """The external_id value that was moved."""

    id_type = Column(StringBackedEnum(IdentifierType), nullable=False)
    """The id_type of the moved external ID."""

    new_recidiviz_id = Column(UUID(as_uuid=True), nullable=False)
    """The new identity the external ID was moved to."""


class SplitEventMovedAttribute(IdentityBase):
    """Attribute value moved off the original identity to one of the new
    identities during a split."""

    __tablename__ = "split_event_moved_attributes"

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    split_event_id = Column(BigInteger, ForeignKey("split_events.id"), nullable=False)
    """The split_event that moved this attribute."""

    attribute_type = Column(StringBackedEnum(AttributeType), nullable=False)
    """Which attribute table the moved row came from (see AttributeType)."""

    attribute_value = Column(JSONB, nullable=False)
    """Serialized SourcedAttributeValue for the moved attribute (typed value
    plus source_type, source_product_app, last_updated_utc)."""

    new_recidiviz_id = Column(UUID(as_uuid=True), nullable=False)
    """The new Identity the attribute was moved to."""


class NoMerge(IdentityBase):
    """Pair of identities the system must never auto-merge."""

    __tablename__ = "no_merge"

    __table_args__ = (
        # Pairs are stored in canonical order (lower UUID first) so a unique constraint
        # can guard against duplicates regardless of insertion order.
        CheckConstraint(
            "recidiviz_id_a < recidiviz_id_b",
            name="canonical_order",
        ),
        Index(
            "uq_no_merge_pair",
            "recidiviz_id_a",
            "recidiviz_id_b",
            unique=True,
        ),
        # The unique index above also serves lookups starting from recidiviz_id_a.
        # This second index covers the symmetric lookup from recidiviz_id_b, since
        # pairs are stored in a single canonical order (a < b).
        Index("ix_no_merge_recidiviz_id_b", "recidiviz_id_b"),
    )

    id = Column(BigInteger, SaIdentity(), primary_key=True)
    """Auto-generated row identifier."""

    recidiviz_id_a = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """First Identity of the no-merge pair."""

    recidiviz_id_b = Column(
        UUID(as_uuid=True),
        ForeignKey("identities.recidiviz_id"),
        nullable=False,
    )
    """Second Identity of the no-merge pair."""

    reason = Column(String, nullable=True)
    """Free-form human explanation of why these two must not be merged."""

    created_by = Column(String, nullable=False)
    """Email of the authenticated user who recorded this no-merge pair."""

    created_utc = Column(DateTime, nullable=False)
    """When the no-merge entry was created."""
