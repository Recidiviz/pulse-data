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
"""Tests that enforce the burndown of deployed view references to legacy TOMIS
1.0 raw data as part of the US_TN migration to the MiCase OMS (TOMIS 2.0).
"""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.regions.us_tn.us_tn_tomis_migration_file_tags import (
    LEGACY_TOMIS_FILE_TAGS,
    MICASE_FILE_TAGS,
)
from recidiviz.tests.view_registry.us_tn_tomis_migration_exemptions import (
    US_TN_LEGACY_TOMIS_REFERENCE_EXEMPTIONS,
)
from recidiviz.tools.docs.us_tn_tomis1_burndown.us_tn_tomis_migration_reporting import (
    legacy_tomis_references_for_project,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class UsTnTomisFileTagClassificationTest(unittest.TestCase):
    """Tests that the US_TN raw data file tag classification in
    us_tn_tomis_migration_file_tags.py stays in sync with the actual US_TN raw
    data configs.
    """

    def test_all_us_tn_raw_file_tags_classified(self) -> None:
        """Tests that every US_TN raw data file tag is explicitly classified as
        either a legacy TOMIS 1.0 file or a MiCase (TOMIS 2.0) file, and that
        neither classification set contains tags for files that no longer
        exist.
        """
        actual_tags = set(
            get_region_raw_file_config(StateCode.US_TN.value).raw_file_configs
        )

        errors = []
        if doubly_classified := LEGACY_TOMIS_FILE_TAGS & MICASE_FILE_TAGS:
            errors.append(
                f"Found file tags in both LEGACY_TOMIS_FILE_TAGS and "
                f"MICASE_FILE_TAGS. Each tag must appear in exactly one set: "
                f"{sorted(doubly_classified)}"
            )
        if unclassified := actual_tags - LEGACY_TOMIS_FILE_TAGS - MICASE_FILE_TAGS:
            errors.append(
                f"Found US_TN raw data file tags that are not classified in "
                f"us_tn_tomis_migration_file_tags.py. New US_TN raw data files should "
                f"never contain legacy TOMIS 1.0 data -- add these to "
                f"MICASE_FILE_TAGS: {sorted(unclassified)}"
            )
        if nonexistent := (LEGACY_TOMIS_FILE_TAGS | MICASE_FILE_TAGS) - actual_tags:
            errors.append(
                f"Found classified file tags with no corresponding US_TN raw "
                f"data config. Remove these from the sets in "
                f"us_tn_tomis_migration_file_tags.py: {sorted(nonexistent)}"
            )
        if errors:
            self.fail("\n\n".join(errors))


class UsTnTomisMigrationBurndownTestBase(unittest.TestCase):
    """Tests that no deployed view query in a single GCP project would
    reference legacy TOMIS 1.0 raw data if the tomis_2_0_enabled feature flag
    were turned on, unless that reference is explicitly exempted. Subclasses
    run the test for each deployed project.
    """

    __test__ = False

    project_id: str

    def test_no_legacy_tomis_references_with_tomis_2_0_enabled(self) -> None:
        """Tests that no deployed view query would reference a legacy TOMIS 1.0
        raw data table, *_latest view, or *_all view if the tomis_2_0_enabled
        feature flag were turned on, unless that reference is explicitly
        exempted in US_TN_LEGACY_TOMIS_REFERENCE_EXEMPTIONS.
        """
        actual_references = legacy_tomis_references_for_project(self.project_id)

        errors = []
        for deprecated_address in sorted(actual_references, key=lambda a: a.to_str()):
            non_exempted = actual_references[deprecated_address] - set(
                US_TN_LEGACY_TOMIS_REFERENCE_EXEMPTIONS.get(deprecated_address, {})
            )
            if non_exempted:
                errors.append(
                    f"  Legacy TOMIS 1.0 address: {deprecated_address.to_str()}\n"
                    f"  Referenced by (non-exempted):"
                    + BigQueryAddress.addresses_to_str(non_exempted, indent_level=4)
                )

        if errors:
            self.fail(
                f"Found views in project [{self.project_id}] that would "
                f"reference legacy TOMIS 1.0 raw data if the tomis_2_0_enabled "
                f"feature flag were turned on. New references should read from "
                f"MiCase (TOMIS 2.0) raw data instead; if a reference cannot "
                f"be avoided, add an exemption in "
                f"us_tn_tomis_migration_exemptions.py.\n\n" + "\n\n".join(errors)
            )


class UsTnTomisMigrationBurndownStagingTest(UsTnTomisMigrationBurndownTestBase):
    __test__ = True

    project_id = GCP_PROJECT_STAGING


class UsTnTomisMigrationBurndownProductionTest(UsTnTomisMigrationBurndownTestBase):
    __test__ = True

    project_id = GCP_PROJECT_PRODUCTION


class UsTnTomisStaleExemptionsTest(unittest.TestCase):
    """Tests that every exempted reference to legacy TOMIS 1.0 raw data still
    exists, so that the exemption list can only shrink over time.
    """

    def test_no_stale_exemptions(self) -> None:
        """Tests that every entry in US_TN_LEGACY_TOMIS_REFERENCE_EXEMPTIONS is
        still an actual reference in at least one deployed project. Checks
        across all projects so that an exemption for a view deployed in only
        one project (e.g. a staging-only view) is not flagged as stale in the
        others.
        """
        references_by_project = [
            legacy_tomis_references_for_project(project_id)
            for project_id in (GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION)
        ]

        errors = []
        for deprecated_address in sorted(
            US_TN_LEGACY_TOMIS_REFERENCE_EXEMPTIONS, key=lambda a: a.to_str()
        ):
            actual_references: set[BigQueryAddress] = set().union(
                *(
                    references.get(deprecated_address, frozenset())
                    for references in references_by_project
                )
            )
            stale_exemptions = (
                set(US_TN_LEGACY_TOMIS_REFERENCE_EXEMPTIONS[deprecated_address])
                - actual_references
            )
            if stale_exemptions:
                errors.append(
                    f"  Legacy TOMIS 1.0 address: {deprecated_address.to_str()}\n"
                    f"  Stale exemptions (no longer referenced in any project):"
                    + BigQueryAddress.addresses_to_str(stale_exemptions, indent_level=4)
                )

        if errors:
            self.fail(
                "Found exemptions in us_tn_tomis_migration_exemptions.py for "
                "references to legacy TOMIS 1.0 raw data that no longer exist. "
                "Nice work migrating them! Remove the stale entries from the "
                "exemption list.\n\n" + "\n\n".join(errors)
            )
