# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

# pylint: disable=unused-import,wrong-import-order

"""Tests for utils/environment.py."""


import pytest

from recidiviz.utils import secrets


@pytest.mark.usefixtures("emulator")
class TestSecrets:
    """Tests for the secrets module."""

    def teardown_method(self, _test_method):
        secrets.clear_secrets()
        secrets.CACHED_SECRETS.clear()

    def test_get_in_cache(self):
        write_to_local('top_track', 'Olson')

        actual = secrets.get_secret('top_track')
        assert actual == 'Olson'

    def test_get_in_datastore(self):
        secrets.set_secret('top_track', 'An Eagle In Your Mind')

        actual = secrets.get_secret('top_track')
        assert actual == 'An Eagle In Your Mind'

    def test_get_in_neither_with_different_cahce_and_datastore(self):
        write_to_local('top_track', 'Wildlife Analysis')
        write_to_local('solid_track', 'Telephasic Workshop')

        actual = secrets.get_secret('other_track')
        assert actual is None

    def test_get_in_datastore_with_different_cache(self):
        write_to_local('top_track', 'Wildlife Analysis')
        secrets.set_secret('solid_track', 'Kaini Industries')

        actual = secrets.get_secret('solid_track')
        assert actual == 'Kaini Industries'

    def test_get_in_neither(self):
        actual = secrets.get_secret('top_track')
        assert actual is None

    def test_set_overwrite(self):
        secrets.set_secret('top_track', 'An Eagle In Your Mind')
        secrets.set_secret('top_track', 'Kaini Industries')

        assert secrets.get_secret('top_track') == 'Kaini Industries'

    def test_clear(self):
        secrets.set_secret('top_track', 'An Eagle In Your Mind')
        secrets.set_secret('solid_track', 'Kaini Industries')

        secrets.clear_secrets()

        assert secrets.get_secret('top_track') is None
        assert secrets.get_secret('solid_track') is None


def write_to_local(name, value):
    secrets.CACHED_SECRETS[name] = value
