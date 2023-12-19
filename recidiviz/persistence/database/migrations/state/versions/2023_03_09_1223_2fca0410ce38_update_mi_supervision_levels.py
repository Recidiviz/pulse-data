# pylint: skip-file
"""update_mi_supervision_levels

Revision ID: 2fca0410ce38
Revises: fd973fd2dbdd
Create Date: 2023-03-09 12:23:59.890129

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2fca0410ce38"
down_revision = "fd973fd2dbdd"
branch_labels = None
depends_on = None


# THIS MIGRATION REMAPS THE BELOW SUPERVISION LEVEL RAW TEXTS FROM INTERNAL_UNKNOWN TO THE FOLLOWING:
#   StateSupervisionLevel.WARRANT:
#     - "2286" # Warrant Status
#     - "2394" # Absconder Warrant Status
#   StateSupervisionLevel.ABSCONSION:
#     - "7483" # Probation Absconder Warrant Status
#     - "7482" # Parole Absconder Warrant Status
#   StateSupervisionLevel.IN_CUSTODY:
#     - "7405" # Paroled in Custody

# THIS MIGRATION REMAPS THE BELOW SUPERVISION LEVEL RAW TEXTS FROM MINIMUM TO THE FOLLOWING:
#   StateSupervisionLevel.IN_CUSTODY:
#     - "2292" # Parole Minimum Administrative
#     - "3624" # Probation Minimum Administrative

# THIS MIGRATION REMAPS THE BELOW SUPERVISION TYPE FROM BENCH_WARRANT TO ABSCONSION:
#   StateSupervisionPeriodSupervisionType.ABSCONSION:
#     - "WARRANT-supervision_level-7483" # Probation Absconder Warrant Status
#     - "WARRANT-supervision_level-7482" # Parole Absconder Warrant Status


def upgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'IN_CUSTODY'
        WHERE supervision_level_raw_text in ('7405', '2292', '3624')
        """
    )

    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'WARRANT'
        WHERE supervision_level_raw_text in ('2394', '2286')
        """
    )

    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'ABSCONSION'
        WHERE supervision_level_raw_text in ('7483', '7482')
        """
    )

    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_type = 'ABSCONSION'
        WHERE supervision_level_raw_text in ('WARRANT-supervision_level-7483', 'WARRANT-supervision_level-7482')
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'INTERNAL_UNKNOWN'
        WHERE supervision_level_raw_text in ('7405', '2394', '2286', '7483', '7482')
        """
    )

    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_level = 'MINIMUM'
        WHERE supervision_level_raw_text in ('2292', '3624')
        """
    )

    op.execute(
        """
        UPDATE state_supervision_period
        SET supervision_type = 'BENCH_WARRANT'
        WHERE supervision_level_raw_text in ('WARRANT-supervision_level-7483', 'WARRANT-supervision_level-7482')
        """
    )
