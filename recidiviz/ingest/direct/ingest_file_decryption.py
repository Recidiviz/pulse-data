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
# =============================================================================
"""Decryption of encrypted direct ingest files delivered by certain states."""
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.secrets import get_secret

ENCRYPTED_INGEST_FILES_STATES = frozenset(
    {
        StateCode.US_NV,
    }
)


def decryption_password_secret_id(state_code: StateCode) -> str:
    """Returns the Secret Manager secret id holding the decryption password for the
    given state's encrypted files.
    """
    return f"{state_code.value.lower()}_ingest_files_decryption_password"


def get_decryption_password_if_encrypted(
    state_code: StateCode, project_id: str
) -> str | None:
    """Returns the decryption password for the given state's interface files, or None if
    the state does not encrypt ingest files.
    """
    if state_code not in ENCRYPTED_INGEST_FILES_STATES:
        return None

    secret_id = decryption_password_secret_id(state_code)
    password = get_secret(secret_id, project_id=project_id)
    if not password:
        raise RuntimeError(
            f"Could not find decryption password secret [{secret_id}] for state "
            f"[{state_code.value}]. Add the password to Secret Manager in project "
            f"[{project_id}] under secret id [{secret_id}] and rerun."
        )
    return password
