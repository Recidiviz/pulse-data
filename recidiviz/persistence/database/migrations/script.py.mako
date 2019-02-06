"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""

# Uncomment the below block if the migration needs to import from any child
# package of recidiviz, otherwise delete

# # Hackity hack to get around the fact that alembic runs this file as a
# # top-level module rather than a child of the recidiviz module
# import sys
# import os
# module_path = os.path.abspath(__file__)
# # Walk up directories to reach main package
# while not module_path.split('/')[-1] == 'pulse-data':
#     if module_path == '/':
#         raise RuntimeError('Top-level recidiviz package not found')
#     module_path = os.path.dirname(module_path)
# sys.path.insert(0, module_path)

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade():
    ${upgrades if upgrades else "pass"}


def downgrade():
    ${downgrades if downgrades else "pass"}
