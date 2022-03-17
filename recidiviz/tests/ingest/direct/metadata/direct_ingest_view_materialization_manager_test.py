# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for DirectIngestViewMaterializationMetadataManager."""
import datetime
import sqlite3
from typing import Type
from unittest import TestCase
from unittest.mock import patch

import attr
import sqlalchemy
from freezegun import freeze_time
from more_itertools import one

from recidiviz.ingest.direct.metadata.direct_ingest_view_materialization_metadata_manager import (
    DirectIngestViewMaterializationMetadataManager,
)
from recidiviz.ingest.direct.types.cloud_task_args import (
    BQIngestViewMaterializationArgs,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.base_entity import Entity, entity_graph_eq
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestViewMaterializationMetadata,
)
from recidiviz.tests.utils import fakes


def _fake_eq(e1: Entity, e2: Entity) -> bool:
    def _should_ignore_field_cb(_: Type, field_name: str) -> bool:
        return field_name == "job_id"

    return entity_graph_eq(e1, e2, _should_ignore_field_cb)


class DirectIngestViewMaterializationMetadataManagerTest(TestCase):
    """Tests for DirectIngestViewMaterializationMetadataManager."""

    def setUp(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        fakes.use_in_memory_sqlite_database(self.database_key)
        self.metadata_manager = DirectIngestViewMaterializationMetadataManager(
            region_code="us_xx", ingest_instance=DirectIngestInstance.PRIMARY
        )

        self.metadata_manager_secondary = (
            DirectIngestViewMaterializationMetadataManager(
                region_code="us_xx", ingest_instance=DirectIngestInstance.SECONDARY
            )
        )

        self.metadata_manager_other_region = (
            DirectIngestViewMaterializationMetadataManager(
                region_code="us_yy", ingest_instance=DirectIngestInstance.PRIMARY
            )
        )

        self.entity_eq_patcher = patch(
            "recidiviz.persistence.entity.operations.entities.OperationsEntity.__eq__",
            _fake_eq,
        )
        self.entity_eq_patcher.start()

    def tearDown(self) -> None:
        self.entity_eq_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    def test_full_job_progression(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

    def test_full_job_progression_two_instances(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        secondary_args = attr.evolve(
            args, ingest_instance=self.metadata_manager_secondary.ingest_instance
        )

        # Running again for a totally different ingest instance should produce same results
        self.run_materialization_job_progression(
            secondary_args,
            self.metadata_manager_secondary,
        )

    def test_clear_materialization_job_metadata(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        self.assertIsNotNone(self.metadata_manager.get_metadata_for_job_args(args))

        # Act
        self.metadata_manager.clear_instance_metadata()

        # Assert
        no_row_found_regex = r"No row was found when one was required"
        with self.assertRaisesRegex(sqlalchemy.exc.NoResultFound, no_row_found_regex):
            self.metadata_manager.get_metadata_for_job_args(args)

    def test_full_job_progression_two_regions(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        self.run_materialization_job_progression(args, self.metadata_manager)

        args = attr.evolve(
            args, ingest_instance=self.metadata_manager_other_region.ingest_instance
        )
        self.run_materialization_job_progression(
            args, self.metadata_manager_other_region
        )

    def test_full_job_progression_same_args_twice_throws(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            f"Attempting to commit repeated DirectIngestViewMaterializationMetadata "
            f"row for region_code={self.metadata_manager.region_code.upper()}, "
            f"instance={self.metadata_manager.ingest_instance.value.upper()}",
        ):
            self.run_materialization_job_progression(args, self.metadata_manager)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = session.query(
                schema.DirectIngestViewMaterializationMetadata
            ).all()
            self.assertEqual(1, len(results))

    def test_full_job_progression_same_args_after_invalidation(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        # Invalidate the previous row
        with SessionFactory.using_database(self.database_key) as session:
            results = session.query(
                schema.DirectIngestViewMaterializationMetadata
            ).all()
            result = one(results)
            result.is_invalidated = True

        # Now we can rerun with the same args
        self.run_materialization_job_progression(args, self.metadata_manager)

    def test_full_job_progression_same_view_name_two_different_dates(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        args_2 = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 3, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args_2, self.metadata_manager)

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = session.query(
                schema.DirectIngestViewMaterializationMetadata
            ).all()

        self.assertEqual(
            {
                args.upper_bound_datetime_to_export,
                args_2.upper_bound_datetime_to_export,
            },
            {r.upper_bound_datetime_inclusive for r in results},
        )
        for r in results:
            self.assertTrue(r.materialization_time)

    def run_materialization_job_progression(
        self,
        job_args: BQIngestViewMaterializationArgs,
        metadata_manager: DirectIngestViewMaterializationMetadataManager,
    ) -> None:
        """Runs through the full progression of operations we expect to run on an
        individual ingest view materialization job.
        """
        # Arrange
        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code=metadata_manager.region_code,
            instance=job_args.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args.upper_bound_datetime_prev,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_to_export,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            metadata_manager.register_ingest_materialization_job(job_args)

        # Assert
        metadata = metadata_manager.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNone(metadata_manager.get_job_completion_time_for_args(job_args))

        # Arrange
        expected_metadata = attr.evolve(
            expected_metadata,
            materialization_time=datetime.datetime(2015, 1, 2, 3, 6, 6),
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            metadata_manager.mark_ingest_view_materialized(job_args)

        # Assert
        metadata = metadata_manager.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)

        self.assertEqual(
            datetime.datetime(2015, 1, 2, 3, 6, 6),
            metadata_manager.get_job_completion_time_for_args(job_args),
        )

    def test_get_most_recent_registered_job_no_jobs(self) -> None:
        self.assertIsNone(
            self.metadata_manager.get_most_recent_registered_job("any_ingest_view")
        )

    def test_get_ingest_view_metadata_for_most_recent_registered_job(self) -> None:
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(
                BQIngestViewMaterializationArgs(
                    ingest_view_name="ingest_view_name",
                    ingest_instance=self.metadata_manager.ingest_instance,
                    upper_bound_datetime_prev=None,
                    upper_bound_datetime_to_export=datetime.datetime(
                        2015, 1, 2, 2, 2, 2, 2
                    ),
                )
            )

        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_materialization_job(
                BQIngestViewMaterializationArgs(
                    ingest_view_name="ingest_view_name",
                    ingest_instance=self.metadata_manager.ingest_instance,
                    upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
                    upper_bound_datetime_to_export=datetime.datetime(
                        2015, 1, 2, 3, 3, 3, 3
                    ),
                )
            )

        with freeze_time("2015-01-02T03:07:07"):
            self.metadata_manager.register_ingest_materialization_job(
                BQIngestViewMaterializationArgs(
                    ingest_view_name="another_ingest_view",
                    ingest_instance=self.metadata_manager.ingest_instance,
                    upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
                    upper_bound_datetime_to_export=datetime.datetime(
                        2015, 1, 2, 3, 4, 4, 4
                    ),
                )
            )

        most_recent_registered_job = (
            self.metadata_manager.get_most_recent_registered_job("ingest_view_name")
        )

        self.assertIsNotNone(most_recent_registered_job)
        if most_recent_registered_job is None:
            self.fail("most_recent_registered_job is unexpectedly None")

        self.assertEqual(
            "ingest_view_name", most_recent_registered_job.ingest_view_name
        )
        self.assertEqual(
            datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            most_recent_registered_job.lower_bound_datetime_exclusive,
        )
        self.assertEqual(
            datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
            most_recent_registered_job.upper_bound_datetime_inclusive,
        )

        # Invalidate the row that was just returned
        with SessionFactory.using_database(self.database_key) as session:
            results = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    ingest_view_name=most_recent_registered_job.ingest_view_name,
                    upper_bound_datetime_inclusive=most_recent_registered_job.upper_bound_datetime_inclusive,
                )
                .all()
            )
            result = one(results)
            result.is_invalidated = True

        most_recent_registered_job = (
            self.metadata_manager.get_most_recent_registered_job("ingest_view_name")
        )
        if most_recent_registered_job is None:
            self.fail("most_recent_registered_job is unexpectedly None")
        self.assertEqual(
            "ingest_view_name", most_recent_registered_job.ingest_view_name
        )
        self.assertEqual(
            None, most_recent_registered_job.lower_bound_datetime_exclusive
        )
        self.assertEqual(
            datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            most_recent_registered_job.upper_bound_datetime_inclusive,
        )

    def test_get_jobs_pending_completion_empty(self) -> None:
        self.assertEqual([], self.metadata_manager.get_jobs_pending_completion())

    def test_get_jobs_pending_completion_basic(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_materialization_job(args)

        expected_list = [
            DirectIngestViewMaterializationMetadata(
                region_code="US_XX",
                instance=self.metadata_manager.ingest_instance,
                ingest_view_name="ingest_view_name",
                is_invalidated=False,
                job_creation_time=datetime.datetime(2015, 1, 2, 3, 6, 6),
                lower_bound_datetime_exclusive=datetime.datetime(
                    2015, 1, 2, 2, 2, 2, 2
                ),
                upper_bound_datetime_inclusive=datetime.datetime(
                    2015, 1, 2, 3, 3, 3, 3
                ),
                materialization_time=None,
            )
        ]

        self.assertEqual(
            expected_list,
            self.metadata_manager.get_jobs_pending_completion(),
        )

    def test_get_jobs_pending_completion_all_completed(self) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_materialization_job(args)

        # ... materialization actually performed in here
        with freeze_time("2015-01-02T03:07:07"):
            self.metadata_manager.mark_ingest_view_materialized(args)

        self.assertEqual([], self.metadata_manager.get_jobs_pending_completion())

    def test_get_jobs_pending_completion_all_completed_in_region(
        self,
    ) -> None:
        args = BQIngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        args_other_region = BQIngestViewMaterializationArgs(
            ingest_view_name="other_ingest_view_name",
            ingest_instance=self.metadata_manager_other_region.ingest_instance,
            upper_bound_datetime_prev=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_to_export=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_materialization_job(args)
            self.metadata_manager_other_region.register_ingest_materialization_job(
                args_other_region
            )

        # ... materialization actually performed in here
        with freeze_time("2015-01-02T03:07:07"):
            self.metadata_manager.mark_ingest_view_materialized(args)

        self.assertEqual([], self.metadata_manager.get_jobs_pending_completion())
