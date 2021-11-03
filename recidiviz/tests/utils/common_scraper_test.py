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

"""Base test class for scrapers."""
from datetime import datetime
from typing import Generic, List, Optional, TypeVar

import yaml
from lxml import html
from mock import patch

from recidiviz.common.constants.person_characteristics import (
    ETHNICITY_MAP,
    Ethnicity,
    Race,
)
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.models import ingest_info
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.single_count import SingleCount
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.utils.individual_ingest_test import IndividualIngestTest

_FAKE_SCRAPER_START_TIME: datetime = datetime(year=2020, month=3, day=20)

ScraperT = TypeVar("ScraperT", bound=BaseScraper)


class CommonScraperTest(Generic[ScraperT], IndividualIngestTest):
    """A base class for scraper tests which does extra validations."""

    def setUp(self) -> None:
        self.scraper: Optional[ScraperT] = None
        self.yaml = None
        self.task_client_patcher = patch("google.cloud.tasks_v2.CloudTasksClient")
        self.task_client_patcher.start()
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "fake-project"

        self._init_scraper_and_yaml()

    def tearDown(self) -> None:
        self.task_client_patcher.stop()
        self.project_id_patcher.stop()

    def _init_scraper_and_yaml(self) -> None:
        """Initialize scraper and yaml. Overridden by most subclasses"""

    def test_scraper_not_none(self) -> None:
        if not self.scraper:
            raise AttributeError("The scraper instance must be set")

    def test_yaml_is_correct(self) -> None:
        if self.yaml:
            with open(self.yaml, "r", encoding="utf-8") as ymlfile:
                manifest = yaml.full_load(ymlfile)

            person = ingest_info.IngestInfo().create_person()
            booking = person.create_booking()
            charge = booking.create_charge()
            arrest = booking.create_arrest()
            sentence = charge.create_sentence()
            bond = charge.create_bond()
            object_verification_map = {
                "person": person,
                "booking": booking,
                "charge": charge,
                "arrest": arrest,
                "bond": bond,
                "sentence": sentence,
            }

            # Validate that key_mappings exists
            if "key_mappings" not in manifest:
                raise AttributeError("key_mappings must exist in the manifest")

            # Make sure there are no unknown keys
            for key in manifest:
                if key not in [
                    "key_mappings",
                    "multi_key_mapping",
                    "keys_to_ignore",
                    "css_key_mappings",
                ]:
                    raise AttributeError(f"Unknown yaml key {key}")

            # Make sure every mapped value in the yaml file exists as a variable
            # in the relevant class.
            for value in manifest["key_mappings"].values():
                class_to_set, attr = value.split(".")
                if attr not in vars(object_verification_map[class_to_set]):
                    raise AttributeError(
                        f"Attribute {attr} is unknown on {class_to_set}, found in key_mappings"
                    )

            if "multi_key_mappings" in manifest:
                for value in manifest["multi_key_mappings"].values():
                    class_to_set, attr = value.split(".")
                    if attr not in vars(object_verification_map[class_to_set]):
                        raise AttributeError(
                            f"Attribute {attr} is unknown on {class_to_set}, found in multi_key_mappings"
                        )

    def test_overrides_are_correct(self) -> None:
        msg = (
            "Default override mappings are not present. Be sure your "
            "scraper's get_enum_overrides calls super() to use "
            "BaseScraper's EnumOverrides."
        )
        if self.scraper is not None:
            overrides = self.scraper.get_enum_overrides()
            for ethnicity_string, ethnicity_enum in ETHNICITY_MAP.items():
                if ethnicity_enum is Ethnicity.HISPANIC:
                    assert (
                        overrides.parse(ethnicity_string, Race) == ethnicity_enum
                    ), msg

    def validate_and_return_get_more_tasks(
        self,
        content: Optional[html.HtmlElement],
        task: Task,
        expected_result: List[Task],
    ) -> List[Task]:
        """This function runs get more tasks and runs some extra validation
        on the output.

        Args:
            content: the content of the page to pass into get_more_tasks
            params: the params to pass into get_more_tasks
            expected_result: the result we expect from the call

        Returns:
            The result from get_more_tasks in case the user needs to do any
            extra validations on the output.
        """
        result: List[Task]
        if self.scraper:
            result = self.scraper.get_more_tasks(content, task)
        assert result == expected_result
        return result

    def validate_and_return_populate_data(
        self,
        content: Optional[html.HtmlElement],
        expected_ingest_info: Optional[IngestInfo] = None,
        expected_single_counts: Optional[List[SingleCount]] = None,
        expected_persist: bool = True,
        task: Optional[Task] = None,
        info: Optional[IngestInfo] = None,
    ) -> ScrapedData:
        """This function runs populate_data and runs some extra validation
        on the output.

        Args:
            content: the content of the page to pass into get_more_tasks
            expected_ingest_info: the ingest info expected to be returned from
                `populate_data`. If `expected_ingest_info` is `None`, then
                expects the return value of `populate_data` to be `None`.
            expected_single_counts: the list of SingleCounts expected to be
            returned from `populate_data`.
            expected_persist: the expected value of persist to be returned from
                `populate_data`.
            task: the task that is being processed, optional.
            info: an ingest_info to use if provided.

        Returns:
            The result from populate_data in case the user needs to do any
            extra validations on the output.
        """
        info_to_ingest: IngestInfo = info or ingest_info.IngestInfo()
        task_to_process: Task = task or Task(
            task_type=constants.TaskType.SCRAPE_DATA, endpoint=""
        )

        if self.scraper:
            scrape_data = self.scraper.populate_data(
                content, task_to_process, info_to_ingest
            )

            print("FINAL")
            print(scrape_data.ingest_info)
            print("EXPECTED")
            print(expected_ingest_info)

            if expected_ingest_info is None and expected_single_counts is None:
                if scrape_data:
                    assert scrape_data.persist is False
                else:
                    assert scrape_data is None

            if expected_single_counts and scrape_data.single_counts:
                assert len(scrape_data.single_counts) == len(expected_single_counts)
                diff = set(expected_single_counts) ^ set(scrape_data.single_counts)
                assert not diff

            metadata: IngestMetadata = IngestMetadata(
                region=self.scraper.region.region_code,
                jurisdiction_id=self.scraper.region.jurisdiction_id,
                ingest_time=_FAKE_SCRAPER_START_TIME,
                enum_overrides=self.scraper.get_enum_overrides(),
                system_level=SystemLevel.COUNTY,
                database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
            )

            if scrape_data.ingest_info and expected_ingest_info:
                self.validate_ingest(
                    scrape_data.ingest_info, expected_ingest_info, metadata
                )

            assert scrape_data.persist == expected_persist

        if scrape_data:
            return scrape_data
        raise ValueError("Scrape data was not provided ingest info")
