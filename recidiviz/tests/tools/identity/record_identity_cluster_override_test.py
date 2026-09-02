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
"""Tests for record_identity_cluster_override."""
import datetime
import unittest
from unittest.mock import patch

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Sex
from recidiviz.pipelines.ingest.identity.identity_cluster_override import (
    BlessedIdentityValues,
    IdentityClusterOverride,
    IdentityClusterOverrideDisposition,
    IdentityClusterOverrides,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    ConflictCheckedAttribute,
)
from recidiviz.tools.identity.record_identity_cluster_override import (
    _check_no_existing_override,
    _prompt_for_blessed_values,
    record_identity_cluster_override,
)

_TENANT = Tenant.US_XX


class TestPromptForBlessedValues(unittest.TestCase):
    """Tests for _prompt_for_blessed_values."""

    def test_typed_values_are_parsed_into_attribute_types(self) -> None:
        with patch("builtins.input", side_effect=["Rivera", "1980-01-01", "MALE"]):
            values = _prompt_for_blessed_values(
                [
                    ConflictCheckedAttribute.SURNAME,
                    ConflictCheckedAttribute.BIRTHDATE,
                    ConflictCheckedAttribute.SEX,
                ]
            )

        self.assertEqual(
            BlessedIdentityValues(
                surname="Rivera", birthdate=datetime.date(1980, 1, 1), sex=Sex.MALE
            ),
            values,
        )

    def test_empty_response_stores_null(self) -> None:
        """An empty response leaves the attribute None so the kept cluster
        stores null for it, rather than storing an empty string or crashing on
        parsing an empty date or enum value."""
        with patch("builtins.input", side_effect=["", "", "Rivera"]):
            values = _prompt_for_blessed_values(
                [
                    ConflictCheckedAttribute.BIRTHDATE,
                    ConflictCheckedAttribute.SEX,
                    ConflictCheckedAttribute.SURNAME,
                ]
            )

        self.assertEqual(BlessedIdentityValues(surname="Rivera"), values)

    def test_all_empty_responses_store_all_null(self) -> None:
        with patch("builtins.input", side_effect=["", ""]):
            values = _prompt_for_blessed_values(
                [ConflictCheckedAttribute.SURNAME, ConflictCheckedAttribute.SEX]
            )

        self.assertEqual(BlessedIdentityValues(), values)

    def test_enum_values_parse_case_insensitively(self) -> None:
        with patch("builtins.input", side_effect=["male"]):
            values = _prompt_for_blessed_values([ConflictCheckedAttribute.SEX])

        self.assertEqual(BlessedIdentityValues(sex=Sex.MALE), values)

    def test_unparseable_response_reprompts(self) -> None:
        """A response that fails to parse re-prompts for the same attribute
        rather than ending the session."""
        with patch(
            "builtins.input", side_effect=["not-a-date", "1980-01-01", "PIRATE", ""]
        ):
            values = _prompt_for_blessed_values(
                [ConflictCheckedAttribute.BIRTHDATE, ConflictCheckedAttribute.SEX]
            )

        self.assertEqual(
            BlessedIdentityValues(birthdate=datetime.date(1980, 1, 1)), values
        )


class TestCheckNoExistingOverride(unittest.TestCase):
    """Tests for _check_no_existing_override."""

    _EXTERNAL_IDS = (("A", "T1"), ("B", "T2"))

    def _existing_override(self) -> IdentityClusterOverride:
        return IdentityClusterOverride(
            tenant=_TENANT,
            person_type=PersonType.JII,
            external_ids=self._EXTERNAL_IDS,
            disposition=IdentityClusterOverrideDisposition.EXCLUDE,
            blessed_values=None,
            recorded_by="tester",
            recorded_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            note="test",
        )

    def test_existing_override_raises(self) -> None:
        overrides = IdentityClusterOverrides.for_overrides(
            tenant=_TENANT, overrides=[self._existing_override()]
        )
        with patch(
            "recidiviz.tools.identity.record_identity_cluster_override"
            ".read_identity_cluster_overrides",
            return_value=overrides,
        ):
            with self.assertRaisesRegex(ValueError, r"already exists"):
                _check_no_existing_override(
                    project_id="test-project",
                    overrides_dataset_id="us_xx_identity_overrides",
                    tenant=_TENANT,
                    external_ids=self._EXTERNAL_IDS,
                )

    def test_no_existing_override_passes(self) -> None:
        with patch(
            "recidiviz.tools.identity.record_identity_cluster_override"
            ".read_identity_cluster_overrides",
            return_value=IdentityClusterOverrides.empty(_TENANT),
        ):
            _check_no_existing_override(
                project_id="test-project",
                overrides_dataset_id="us_xx_identity_overrides",
                tenant=_TENANT,
                external_ids=self._EXTERNAL_IDS,
            )


class TestRecordIdentityClusterOverride(unittest.TestCase):
    """Tests for record_identity_cluster_override."""

    def test_invalid_external_id_type_raises_before_bigquery(self) -> None:
        """An unregistered external id type fails fast, before the tool
        constructs a BigQuery client or reads any table."""
        with patch(
            "recidiviz.tools.identity.record_identity_cluster_override"
            ".BigQueryClientImpl"
        ) as mock_client:
            with self.assertRaisesRegex(
                ValueError, r"are not registered for \[US_ND\]"
            ):
                record_identity_cluster_override(
                    tenant=Tenant.US_ND,
                    disposition=IdentityClusterOverrideDisposition.EXCLUDE,
                    external_ids=(("12345", "US_ND_DOC"),),
                    person_type_arg=PersonType.JII,
                    recorded_by="tester",
                    note_arg="test",
                    project_id="test-project",
                    sandbox_dataset_prefix=None,
                )
        mock_client.assert_not_called()
