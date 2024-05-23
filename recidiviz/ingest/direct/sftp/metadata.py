# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Metadata related to SFTP"""

DISABLED_ALGORITHMS_KWARG = "disabled_algorithms"
# A recent change to Paramiko defined a preference order for algorithms that are used to
# decrypt host keys and private keys. For RSA keys, we have identified that these two
# algorithms take precedent over ssh-rsa, which is the algorithm preferred by state
# partners. Therefore, if there are other channel closed errors, we should consider
# adding algorithms to this list in order to force preference of the algorithms that our
# hostkey secrets contain by our state partners.
SFTP_DISABLED_ALGORITHMS_PUB_KEYS = {"pubkeys": ["rsa-sha2-512", "rsa-sha2-256"]}
