#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2026 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
#
"""Define the ORM schema for data persisted for eventual reingestion."""
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import DeclarativeMeta, declarative_base

from recidiviz.persistence.database.database_entity import DatabaseEntity

# Key to pass to __table_args__["info"] to keep the table up-to-date with alembic migrations.
RUN_MIGRATIONS = "run_migrations"

# Defines the base class for all table classes in the Outliers schema.
PersistenceBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="PersistenceBase"
)


class UsTnCafFormEdits(PersistenceBase):
    """Updates to CAF forms entered via Recidiviz tools such as Workflows"""

    __tablename__ = "us_tn_caf_edits"

    # Manage this table via alembic
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    # Primary Keys
    state_code = Column(String, primary_key=True)
    OFFENDERID = Column(String, primary_key=True, nullable=False)
    ClassificationType = Column(String, primary_key=True, nullable=False)
    ClassificationFormType = Column(String, primary_key=True, nullable=False)
    AssessmentDate = Column(DateTime, primary_key=True, nullable=False)

    # Required columns
    LastClassificationDate = Column(DateTime, nullable=False)
    LastModifiedBy = Column(String, nullable=False)

    # Nullable columns
    Question1 = Column(Integer, nullable=True)
    Question1_notes = Column(String, nullable=True)
    Question2 = Column(Integer, nullable=True)
    Question2_notes = Column(String, nullable=True)
    Question3 = Column(Integer, nullable=True)
    Question3_notes = Column(String, nullable=True)
    Question4 = Column(Integer, nullable=True)
    Question4_notes = Column(String, nullable=True)
    Question5 = Column(Integer, nullable=True)
    Question5_notes = Column(String, nullable=True)
    Question6 = Column(Integer, nullable=True)
    Question6_notes = Column(String, nullable=True)
    Question7 = Column(Integer, nullable=True)
    Question7_notes = Column(String, nullable=True)
    OverallScore = Column(Integer, nullable=True)
    ScoredCustodyLevel = Column(String, nullable=True)
    CounselorRecommendedOverride = Column(String, nullable=True)
    CounselorRecommendedCustodyLevel = Column(String, nullable=True)
    FinalOverrideCode = Column(String, nullable=True)
    FinalCustodyLevel = Column(String, nullable=True)
    DateOfApprovalAndEntry_CAF = Column(DateTime, nullable=True)

    TrusteeFlag = Column(String, nullable=True)
    Trustee_Question1 = Column(String, nullable=True)
    Trustee_Question2 = Column(String, nullable=True)
    Trustee_Question3 = Column(String, nullable=True)
    Trustee_Question4 = Column(String, nullable=True)
    Trustee_Question5 = Column(String, nullable=True)
    Trustee_Question6 = Column(String, nullable=True)
    Trustee_Question7 = Column(String, nullable=True)
    Trustee_Question8 = Column(String, nullable=True)
    Trustee_Question9 = Column(String, nullable=True)
    Trustee_Question10 = Column(String, nullable=True)
    Trustee_Question11 = Column(String, nullable=True)
    Trustee_Question12 = Column(String, nullable=True)
    Trustee_Question13 = Column(String, nullable=True)
    DateOfApprovalAndEntry_Trustee = Column(DateTime, nullable=True)
    TrusteeApprovedOrDenied = Column(String, nullable=True)
    TrusteeEligible = Column(String, nullable=True)
    TrusteeDenialReasons = Column(String, nullable=True)
    Warden_TrusteeSignaturesAcquired = Column(String, nullable=True)
    Warden_TrusteeSignaturesAcquiredDate = Column(DateTime, nullable=True)
    ContractMonitor_TrusteeSignaturesAcquired = Column(String, nullable=True)
    ContractMonitor_TrusteeSignaturesAcquiredDate = Column(DateTime, nullable=True)
    AC_TrusteeSignaturesAcquired = Column(String, nullable=True)
    AC_TrusteeSignaturesAcquiredDate = Column(DateTime, nullable=True)
    ChiefCounselorFinalizingForm = Column(String, nullable=True)
    TrusteeChecklistComplete = Column(String, nullable=True)

    DateOfFinalApprovalAndEntry = Column(DateTime, nullable=True)
