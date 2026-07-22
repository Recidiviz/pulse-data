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
"""Guards that the edovo-wif@ service account is never granted a role.

The Edovo WIF security model depends on edovo-wif@ having zero permissions — the
federated token is only an identity presented to the endpoint, not a key to any
GCP resource. This fails if any Terraform IAM resource lists the SA as a member.
"""
import glob
import os
import re
import unittest

import recidiviz.tools.deploy as deploy_pkg

_TF_DIR = os.path.join(os.path.dirname(deploy_pkg.__file__), "terraform")

# The edovo-wif@ SA is used as an IAM member (i.e. granted a role) only via its
# `.member` attribute or a `serviceAccount:<its email>` principal. Its legitimate
# uses — the resource definition, the workloadIdentityUser binding scoped to it,
# its unique_id/email outputs — never take these forms.
_SA_AS_MEMBER_PATTERNS = [
    re.compile(r"google_service_account\.edovo\.member"),
    re.compile(r"serviceAccount:\$\{google_service_account\.edovo\.email\}"),
    re.compile(r"serviceAccount:edovo-wif@"),
]


class EdovoWifNoRolesTest(unittest.TestCase):
    """Asserts no Terraform grants the edovo-wif@ SA any role."""

    def test_edovo_wif_sa_is_never_an_iam_member(self) -> None:
        offending: list[str] = []
        for path in sorted(glob.glob(os.path.join(_TF_DIR, "*.tf"))):
            with open(path, encoding="utf-8") as tf_file:
                text = tf_file.read()
            for pattern in _SA_AS_MEMBER_PATTERNS:
                if pattern.search(text):
                    offending.append(f"{os.path.basename(path)}: {pattern.pattern}")

        self.assertEqual(
            offending,
            [],
            "edovo-wif@ must not be granted any role — it is an identity-only "
            f"account. Found it used as an IAM member: {offending}. Remove the "
            "grant (edovo-wif.tf, OBT-23211).",
        )
