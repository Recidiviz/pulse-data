# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests checking that all the yamls deployed by our builds parse successfully.
"""
import os
import re
import unittest

import deepdiff

from recidiviz.utils.yaml_dict import YAMLDict


class TestDeployYamls(unittest.TestCase):
    """Tests checking that all the yamls deployed by our builds parse
    successfully.
    """

    def path_for_build_file(self, file_name: str) -> str:
        return os.path.join(os.path.dirname(__file__), "..", "..", file_name)

    def test_cron_yaml_parses(self) -> None:
        yaml_dict = YAMLDict.from_path(self.path_for_build_file("cron.yaml"))
        self.assertTrue(yaml_dict.get())

    def test_prod_yaml_parses(self) -> None:
        yaml_dict = YAMLDict.from_path(self.path_for_build_file("prod.yaml"))
        self.assertTrue(yaml_dict.get())

    def test_staging_yaml_parses(self) -> None:
        yaml_dict = YAMLDict.from_path(self.path_for_build_file("staging.yaml"))
        self.assertTrue(yaml_dict.get())

    def test_prod_staging_same(self) -> None:
        staging_yaml = YAMLDict.from_path(self.path_for_build_file("staging.yaml"))
        prod_yaml = YAMLDict.from_path(self.path_for_build_file("prod.yaml"))

        diff = deepdiff.DeepDiff(staging_yaml.get(), prod_yaml.get())

        # We expect the RECIDIVIZ_ENV values to be different
        env_diff = diff["values_changed"].pop("root['env_variables']['RECIDIVIZ_ENV']")
        self.assertEqual({"new_value": "production", "old_value": "staging"}, env_diff)

        # We expect the cloud sql instance names to be different, but names should match same pattern
        cloud_sql_instance_diff = diff["values_changed"].pop(
            "root['beta_settings']['cloud_sql_instances']"
        )
        staging_cloud_sql_instances = cloud_sql_instance_diff["old_value"].split(", ")
        prod_cloud_sql_instances = cloud_sql_instance_diff["new_value"].split(", ")

        for i, instance in enumerate(staging_cloud_sql_instances):
            instance = instance.replace("recidiviz-staging", "recidiviz-123").replace(
                "dev-", "prod-"
            )
            match = re.match("(.*-data).*", instance)
            if match:
                staging_cloud_sql_instances[i] = match.group(1)
            else:
                staging_cloud_sql_instances[i] = instance

        for i, instance in enumerate(prod_cloud_sql_instances):
            match = re.match("(.*-data).*", instance)
            if match:
                prod_cloud_sql_instances[i] = match.group(1)
            else:
                prod_cloud_sql_instances[i] = instance

        self.assertCountEqual(staging_cloud_sql_instances, prod_cloud_sql_instances)

        vpc_connector_diff = diff["values_changed"].pop(
            "root['vpc_access_connector']['name']"
        )
        staging_vpc_connector = vpc_connector_diff["old_value"]
        production_vpc_connector = vpc_connector_diff["new_value"]
        self.assertEqual(
            staging_vpc_connector.replace(
                "recidiviz-staging", "recidiviz-123"
            ),  # Staging becomes production
            production_vpc_connector,
        )

        # We expect the AIRFLOW_URI values to be different, with no meaningful relation to the
        # project IDs.
        airflow_uri_diff = diff["values_changed"].pop(
            "root['env_variables']['AIRFLOW_URI']"
        )
        staging_airflow_uri = airflow_uri_diff["old_value"]
        production_airflow_uri = airflow_uri_diff["new_value"]
        self.assertEqual(
            staging_airflow_uri.split("-", 1)[
                1
            ],  # URIs should be the same after the first "-"
            production_airflow_uri.split("-", 1)[1],
        )

        # There should be no other values changed between the two
        self.assertFalse(diff.pop("values_changed"))
        # Aside from the few values changed, there should be no other changes
        self.assertFalse(diff)
