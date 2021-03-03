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

"""Tests for utils/environment.py."""
import json
from datetime import datetime
import unittest
import pytest

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.persistence import datastore_ingest_info
from recidiviz.ingest.scrape import constants

TEST_ENDPOINT = "www.test.com"


def sample_ingest_info(number: str) -> IngestInfo:
    ingest_info = IngestInfo()

    person = ingest_info.create_person()
    person.full_name = "LAST NAME, FIRST NAME MIDDLE NAME"
    person.person_id = number
    return ingest_info


@pytest.mark.usefixtures("emulator")
class TestDatastoreIngestInfo(unittest.TestCase):
    """Tests for the DatastoreIngestInfo module."""

    def test_write_single_ingest_info(self):
        task_hash = hash(
            json.dumps(
                Task(
                    task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint=TEST_ENDPOINT,
                    response_type=constants.ResponseType.TEXT,
                ).to_serializable(),
                sort_keys=True,
            )
        )

        start_time = datetime.now()
        ingest_info = datastore_ingest_info.write_ingest_info(
            region="us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("1"),
            task_hash=task_hash,
        )
        results = datastore_ingest_info.batch_get_ingest_infos_for_region(
            "us_state_county", start_time
        )
        assert results == [ingest_info]
        datastore_ingest_info.batch_delete_ingest_infos_for_region("us_state_county")

    def test_batch_get_ingest_infos_for_region(self):
        task_hash = hash(
            json.dumps(
                Task(
                    task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint=TEST_ENDPOINT,
                    response_type=constants.ResponseType.TEXT,
                ).to_serializable(),
                sort_keys=True,
            )
        )

        start_time = datetime.now()
        first = datastore_ingest_info.write_ingest_info(
            region="us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("1"),
            task_hash=task_hash,
        )
        second = datastore_ingest_info.write_ingest_info(
            region="us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("2"),
            task_hash=task_hash,
        )
        third = datastore_ingest_info.write_ingest_info(
            region="us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("3"),
            task_hash=task_hash,
        )
        datastore_ingest_info.write_ingest_info(
            region="unrelated",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("n/a"),
            task_hash=task_hash,
        )

        results = datastore_ingest_info.batch_get_ingest_infos_for_region(
            "us_state_county", start_time
        )

        assert results == [first, second, third]
        datastore_ingest_info.batch_delete_ingest_infos_for_region("us_state_county")

    def test_batch_delete_ingest_infos_for_region(self):
        task_hash = hash(
            json.dumps(
                Task(
                    task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint=TEST_ENDPOINT,
                    response_type=constants.ResponseType.TEXT,
                ).to_serializable(),
                sort_keys=True,
            )
        )
        start_time = datetime.now()

        datastore_ingest_info.write_ingest_info(
            region="us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("1"),
            task_hash=task_hash,
        )
        datastore_ingest_info.write_ingest_info(
            region="us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("2"),
            task_hash=task_hash,
        )
        datastore_ingest_info.write_ingest_info(
            region="us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("3"),
            task_hash=task_hash,
        )
        unrelated = datastore_ingest_info.write_ingest_info(
            region="unrelated_us_state_county",
            session_start_time=start_time,
            ingest_info=sample_ingest_info("n/a"),
            task_hash=task_hash,
        )

        datastore_ingest_info.batch_delete_ingest_infos_for_region("us_state_county")

        assert (
            datastore_ingest_info.batch_get_ingest_infos_for_region(
                "us_state_county", start_time
            )
            == []
        )

        actual = datastore_ingest_info.batch_get_ingest_infos_for_region(
            "unrelated_us_state_county", start_time
        )
        assert actual == [unrelated]

        datastore_ingest_info.batch_delete_ingest_infos_for_region(
            "unrelated_us_state_county"
        )

    def test_batch_delete_over_500_ingest_infos_for_region(self):
        task_hash = hash(
            json.dumps(
                Task(
                    task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint=TEST_ENDPOINT,
                    response_type=constants.ResponseType.TEXT,
                ).to_serializable(),
                sort_keys=True,
            )
        )
        start_time = datetime.now()

        # The Datastore limit for entity writes in one call is 500. Confirm
        # that batch delete is properly handled when more than 500 entities
        # exist for the same region.
        for i in range(600):
            datastore_ingest_info.write_ingest_info(
                region="us_state_county",
                session_start_time=start_time,
                ingest_info=sample_ingest_info(str(i)),
                task_hash=task_hash,
            )

        datastore_ingest_info.batch_delete_ingest_infos_for_region("us_state_county")

        assert (
            datastore_ingest_info.batch_get_ingest_infos_for_region(
                "us_state_county", start_time
            )
            == []
        )

    def test_write_errors(self):
        task_hash = hash(
            json.dumps(
                Task(
                    task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint=TEST_ENDPOINT,
                    response_type=constants.ResponseType.TEXT,
                ).to_serializable(),
                sort_keys=True,
            )
        )

        start_time = datetime.now()
        batch_ingest_info_data = datastore_ingest_info.write_error(
            region="us_state_county",
            session_start_time=start_time,
            error="error string",
            trace_id="trace",
            task_hash=task_hash,
        )

        results = datastore_ingest_info.batch_get_ingest_infos_for_region(
            "us_state_county", start_time
        )

        assert results == [batch_ingest_info_data]
        datastore_ingest_info.batch_delete_ingest_infos_for_region("us_state_county")
