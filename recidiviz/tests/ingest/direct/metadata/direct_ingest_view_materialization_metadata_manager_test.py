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
    IngestViewMaterializationSummary,
)
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
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
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

    def test_full_job_progression_two_instances(self) -> None:
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
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
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        self.assertIsNotNone(self.metadata_manager.get_metadata_for_job_args(args))

        # Act
        self.metadata_manager.clear_instance_metadata()

        # Assert
        no_row_found_regex = r"No row was found when one was required"
        with self.assertRaisesRegex(sqlalchemy.exc.NoResultFound, no_row_found_regex):
            self.metadata_manager.get_metadata_for_job_args(args)

    def test_clear_materialization_job_metadata_only_clears_for_region(self) -> None:
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        self.assertIsNotNone(self.metadata_manager.get_metadata_for_job_args(args))

        other_region_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager_other_region.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(
            other_region_args, self.metadata_manager_other_region
        )

        self.assertIsNotNone(
            self.metadata_manager_other_region.get_metadata_for_job_args(args)
        )

        # Act
        self.metadata_manager.clear_instance_metadata()

        # Assert
        no_row_found_regex = r"No row was found when one was required"
        with self.assertRaisesRegex(sqlalchemy.exc.NoResultFound, no_row_found_regex):
            self.metadata_manager.get_metadata_for_job_args(args)

        self.assertIsNotNone(
            self.metadata_manager_other_region.get_metadata_for_job_args(args)
        )

    def test_clear_materialization_job_metadata_only_clears_for_instance(self) -> None:
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        self.assertIsNotNone(self.metadata_manager.get_metadata_for_job_args(args))

        other_instance_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager_secondary.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(
            other_instance_args, self.metadata_manager_secondary
        )

        self.assertIsNotNone(
            self.metadata_manager_secondary.get_metadata_for_job_args(args)
        )

        # Act
        self.metadata_manager.clear_instance_metadata()

        # Assert
        no_row_found_regex = r"No row was found when one was required"
        with self.assertRaisesRegex(sqlalchemy.exc.NoResultFound, no_row_found_regex):
            self.metadata_manager.get_metadata_for_job_args(args)

        self.assertIsNotNone(
            self.metadata_manager_secondary.get_metadata_for_job_args(args)
        )

    def test_full_job_progression_two_regions(self) -> None:
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        self.run_materialization_job_progression(args, self.metadata_manager)

        args = attr.evolve(
            args, ingest_instance=self.metadata_manager_other_region.ingest_instance
        )
        self.run_materialization_job_progression(
            args, self.metadata_manager_other_region
        )

    def test_full_job_progression_same_args_twice_throws(self) -> None:
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
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
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
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
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        self.run_materialization_job_progression(args, self.metadata_manager)

        args_2 = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 3, 3, 3, 3, 3),
        )

        self.run_materialization_job_progression(
            args_2, self.metadata_manager, num_previous_jobs_completed_for_view=1
        )

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            results = session.query(
                schema.DirectIngestViewMaterializationMetadata
            ).all()

        self.assertEqual(
            {
                args.upper_bound_datetime_inclusive,
                args_2.upper_bound_datetime_inclusive,
            },
            {r.upper_bound_datetime_inclusive for r in results},
        )
        for r in results:
            self.assertTrue(r.materialization_time)

    def run_materialization_job_progression(
        self,
        job_args: IngestViewMaterializationArgs,
        metadata_manager: DirectIngestViewMaterializationMetadataManager,
        num_previous_jobs_completed_for_view: int = 0,
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
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
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

        summaries = metadata_manager.get_instance_summaries()
        if job_args.ingest_view_name not in summaries:
            raise ValueError(
                f"Expected value for [{job_args.ingest_view_name}] in the summaries"
            )

        self.assertEqual(
            IngestViewMaterializationSummary(
                ingest_view_name=job_args.ingest_view_name,
                num_pending_jobs=0,
                num_completed_jobs=num_previous_jobs_completed_for_view + 1,
                completed_jobs_max_datetime=job_args.upper_bound_datetime_inclusive,
                pending_jobs_min_datetime=None,
            ),
            summaries[job_args.ingest_view_name],
        )

    def test_get_most_recent_registered_job_no_jobs(self) -> None:
        self.assertIsNone(
            self.metadata_manager.get_most_recent_registered_job("any_ingest_view")
        )

    def test_get_ingest_view_metadata_for_most_recent_registered_job(self) -> None:
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(
                IngestViewMaterializationArgs(
                    ingest_view_name="ingest_view_name",
                    ingest_instance=self.metadata_manager.ingest_instance,
                    lower_bound_datetime_exclusive=None,
                    upper_bound_datetime_inclusive=datetime.datetime(
                        2015, 1, 2, 2, 2, 2, 2
                    ),
                )
            )

        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.register_ingest_materialization_job(
                IngestViewMaterializationArgs(
                    ingest_view_name="ingest_view_name",
                    ingest_instance=self.metadata_manager.ingest_instance,
                    lower_bound_datetime_exclusive=datetime.datetime(
                        2015, 1, 2, 2, 2, 2, 2
                    ),
                    upper_bound_datetime_inclusive=datetime.datetime(
                        2015, 1, 2, 3, 3, 3, 3
                    ),
                )
            )

        with freeze_time("2015-01-02T03:07:07"):
            self.metadata_manager.register_ingest_materialization_job(
                IngestViewMaterializationArgs(
                    ingest_view_name="another_ingest_view",
                    ingest_instance=self.metadata_manager.ingest_instance,
                    lower_bound_datetime_exclusive=datetime.datetime(
                        2015, 1, 2, 3, 3, 3, 3
                    ),
                    upper_bound_datetime_inclusive=datetime.datetime(
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
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
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
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
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
        args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
        )
        args_other_region = IngestViewMaterializationArgs(
            ingest_view_name="other_ingest_view_name",
            ingest_instance=self.metadata_manager_other_region.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 3, 3, 3, 3),
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

    def test_mark_instance_data_invalidated_primary_instance(self) -> None:
        # Arrange
        job_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager.region_code,
            instance=job_args.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(job_args)

        # Assert
        metadata = self.metadata_manager.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNone(
            self.metadata_manager.get_job_completion_time_for_args(job_args)
        )

        # Arrange
        expected_metadata = attr.evolve(
            expected_metadata,
            is_invalidated=True,
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.mark_instance_data_invalidated()

        # Assert
        # metadata = self.metadata_manager.get_metadata_for_job_args(job_args)
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            all_metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager.region_code.upper(),
                    instance=job_args.ingest_instance.value,
                )
                .all()
            )
            for metadata in all_metadata:
                # Check here that found_metadata has expected items and all are marked as invalidated.
                self.assertEqual(
                    expected_metadata, convert_schema_object_to_entity(metadata)
                )

    def test_mark_instance_data_invalidated_secondary_instance(self) -> None:
        # Arrange
        job_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager_secondary.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager_secondary.region_code,
            instance=job_args.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager_secondary.register_ingest_materialization_job(
                job_args
            )

        # Assert
        metadata = self.metadata_manager_secondary.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNone(
            self.metadata_manager_secondary.get_job_completion_time_for_args(job_args)
        )

        # Arrange
        expected_metadata = attr.evolve(
            expected_metadata,
            is_invalidated=True,
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager_secondary.mark_instance_data_invalidated()

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager_secondary.region_code.upper(),
                    instance=job_args.ingest_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all are marked as invalidated.
            self.assertEqual(
                expected_metadata, convert_schema_object_to_entity(metadata)
            )

    def test_mark_instance_data_invalidated_multiple_instances(self) -> None:
        # Arrange
        job_args_primary = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name_primary",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        job_args_secondary = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name_secondary",
            ingest_instance=self.metadata_manager_secondary.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 3),
        )

        expected_primary_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager.region_code,
            instance=job_args_primary.ingest_instance,
            ingest_view_name=job_args_primary.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args_primary.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args_primary.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        expected_secondary_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager_secondary.region_code,
            instance=job_args_secondary.ingest_instance,
            ingest_view_name=job_args_secondary.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args_secondary.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args_secondary.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(job_args_primary)
            self.metadata_manager_secondary.register_ingest_materialization_job(
                job_args_secondary
            )

        # Assert Primary
        metadata_primary = self.metadata_manager.get_metadata_for_job_args(
            job_args_primary
        )
        self.assertEqual(expected_primary_metadata, metadata_primary)
        self.assertIsNone(
            self.metadata_manager.get_job_completion_time_for_args(job_args_primary)
        )

        # Assert Secondary
        metadata_secondary = self.metadata_manager_secondary.get_metadata_for_job_args(
            job_args_secondary
        )
        self.assertEqual(expected_secondary_metadata, metadata_secondary)
        self.assertIsNone(
            self.metadata_manager_secondary.get_job_completion_time_for_args(
                job_args_secondary
            )
        )

        # Arrange
        expected_primary_metadata = attr.evolve(
            expected_primary_metadata,
            is_invalidated=True,
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.mark_instance_data_invalidated()

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            primary_metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager.region_code,
                    instance=job_args_primary.ingest_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all are marked as invalidated.
            self.assertEqual(
                expected_primary_metadata,
                convert_schema_object_to_entity(primary_metadata),
            )

            secondary_metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager_secondary.region_code,
                    instance=job_args_secondary.ingest_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and are NOT marked as invalidated.
            self.assertEqual(
                expected_secondary_metadata,
                convert_schema_object_to_entity(secondary_metadata),
            )

    def test_transfer_metadata_to_new_instance_secondary_to_primary(self) -> None:
        # Arrange
        job_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager_secondary.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager_secondary.region_code,
            instance=job_args.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager_secondary.register_ingest_materialization_job(
                job_args
            )

        # Assert
        metadata = self.metadata_manager_secondary.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNone(
            self.metadata_manager_secondary.get_job_completion_time_for_args(job_args)
        )

        # Arrange
        expected_metadata = attr.evolve(
            expected_metadata,
            instance=self.metadata_manager.ingest_instance,
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager_secondary.transfer_metadata_to_new_instance(
                self.metadata_manager
            )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager.region_code.upper(),
                    instance=self.metadata_manager.ingest_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_metadata, convert_schema_object_to_entity(metadata)
            )

            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                metadata = (
                    session.query(schema.DirectIngestViewMaterializationMetadata)
                    .filter_by(
                        region_code=self.metadata_manager_secondary.region_code.upper(),
                        instance=self.metadata_manager_secondary.ingest_instance.value,
                    )
                    .one()
                )

    def test_transfer_metadata_to_new_instance_primary_to_secondary(self) -> None:
        # Arrange
        job_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager.region_code,
            instance=job_args.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(job_args)

        # Assert
        metadata = self.metadata_manager.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNone(
            self.metadata_manager.get_job_completion_time_for_args(job_args)
        )

        # Arrange
        expected_metadata = attr.evolve(
            expected_metadata,
            instance=self.metadata_manager_secondary.ingest_instance,
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.transfer_metadata_to_new_instance(
                self.metadata_manager_secondary
            )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager_secondary.region_code.upper(),
                    instance=self.metadata_manager_secondary.ingest_instance.value,
                )
                .one()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            self.assertEqual(
                expected_metadata, convert_schema_object_to_entity(metadata)
            )

            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                metadata = (
                    session.query(schema.DirectIngestViewMaterializationMetadata)
                    .filter_by(
                        region_code=self.metadata_manager.region_code.upper(),
                        instance=self.metadata_manager.ingest_instance.value,
                    )
                    .one()
                )

    def test_transfer_data_to_new_instance_multiple(self) -> None:
        # Arrange
        job_args_primary = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        job_args_secondary = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager_secondary.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_primary_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager.region_code,
            instance=job_args_primary.ingest_instance,
            ingest_view_name=job_args_primary.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args_primary.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args_primary.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        expected_secondary_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager_secondary.region_code,
            instance=job_args_secondary.ingest_instance,
            ingest_view_name=job_args_secondary.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args_secondary.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args_secondary.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(job_args_primary)
            self.metadata_manager_secondary.register_ingest_materialization_job(
                job_args_secondary
            )

        # Assert Primary
        metadata_primary = self.metadata_manager.get_metadata_for_job_args(
            job_args_primary
        )
        self.assertEqual(expected_primary_metadata, metadata_primary)
        self.assertIsNone(
            self.metadata_manager.get_job_completion_time_for_args(job_args_primary)
        )

        # Assert Secondary
        metadata_secondary = self.metadata_manager_secondary.get_metadata_for_job_args(
            job_args_secondary
        )
        self.assertEqual(expected_secondary_metadata, metadata_secondary)
        self.assertIsNone(
            self.metadata_manager_secondary.get_job_completion_time_for_args(
                job_args_secondary
            )
        )

        # Arrange
        expected_metadata = attr.evolve(
            expected_secondary_metadata,
            instance=self.metadata_manager.ingest_instance,
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            self.metadata_manager.mark_instance_data_invalidated()
            self.metadata_manager_secondary.transfer_metadata_to_new_instance(
                self.metadata_manager
            )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            all_metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager.region_code.upper(),
                    instance=self.metadata_manager.ingest_instance.value,
                    is_invalidated=False,
                )
                .all()
            )
            # Check here that found_metadata has expected items and all instances are marked primary
            for metadata in all_metadata:
                self.assertEqual(
                    expected_metadata, convert_schema_object_to_entity(metadata)
                )

            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            no_row_found_regex = r"No row was found when one was required"
            with self.assertRaisesRegex(
                sqlalchemy.exc.NoResultFound, no_row_found_regex
            ):
                metadata = (
                    session.query(schema.DirectIngestViewMaterializationMetadata)
                    .filter_by(
                        region_code=self.metadata_manager_secondary.region_code.upper(),
                        instance=self.metadata_manager_secondary.ingest_instance.value,
                    )
                    .one()
                )

    def test_transfer_metadata_to_new_instance_primary_to_primary(self) -> None:
        # Arrange
        job_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager.region_code,
            instance=job_args.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(job_args)

        # Assert
        metadata = self.metadata_manager.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNone(
            self.metadata_manager.get_job_completion_time_for_args(job_args)
        )

        # Act
        with freeze_time("2015-01-02T03:06:06"):
            # Assert that secondary instance was moved to primary instance, thus secondary no longer exists
            same_instance = r"Either state codes are not the same or new instance is same as origin."
            with self.assertRaisesRegex(ValueError, same_instance):
                self.metadata_manager.transfer_metadata_to_new_instance(
                    self.metadata_manager
                )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager.region_code.upper(),
                    instance=self.metadata_manager.ingest_instance.value,
                )
                .one()
            )
            # Check here that origin database is same
            self.assertEqual(
                expected_metadata, convert_schema_object_to_entity(metadata)
            )

    def test_transfer_metadata_to_new_instance_different_states(self) -> None:
        # Arrange
        job_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager_secondary.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager_secondary.region_code,
            instance=job_args.ingest_instance,
            ingest_view_name=job_args.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager_secondary.register_ingest_materialization_job(
                job_args
            )

        # Assert
        metadata = self.metadata_manager_secondary.get_metadata_for_job_args(job_args)
        self.assertEqual(expected_metadata, metadata)
        self.assertIsNone(
            self.metadata_manager_secondary.get_job_completion_time_for_args(job_args)
        )

        # Assert
        with freeze_time("2015-01-02T03:06:06"):
            same_instance = r"Either state codes are not the same or new instance is same as origin."
            with self.assertRaisesRegex(ValueError, same_instance):
                self.metadata_manager_secondary.transfer_metadata_to_new_instance(
                    self.metadata_manager_other_region
                )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager_secondary.region_code.upper(),
                    instance=self.metadata_manager_secondary.ingest_instance.value,
                )
                .one()
            )
            # Check here that origin database is same
            self.assertEqual(
                expected_metadata, convert_schema_object_to_entity(metadata)
            )

    def test_transfer_data_to_new_instance_multiple_raise_exception(self) -> None:
        # Arrange
        job_args_primary = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        job_args_secondary = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager_secondary.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )

        expected_primary_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager.region_code,
            instance=job_args_primary.ingest_instance,
            ingest_view_name=job_args_primary.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args_primary.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args_primary.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        expected_secondary_metadata = DirectIngestViewMaterializationMetadata(
            region_code=self.metadata_manager_secondary.region_code,
            instance=job_args_secondary.ingest_instance,
            ingest_view_name=job_args_secondary.ingest_view_name,
            is_invalidated=False,
            job_creation_time=datetime.datetime(2015, 1, 2, 3, 5, 5),
            lower_bound_datetime_exclusive=job_args_secondary.lower_bound_datetime_exclusive,
            upper_bound_datetime_inclusive=job_args_secondary.upper_bound_datetime_inclusive,
            materialization_time=None,
        )

        # Act
        with freeze_time("2015-01-02T03:05:05"):
            self.metadata_manager.register_ingest_materialization_job(job_args_primary)
            self.metadata_manager_secondary.register_ingest_materialization_job(
                job_args_secondary
            )

        # Assert Primary
        metadata_primary = self.metadata_manager.get_metadata_for_job_args(
            job_args_primary
        )
        self.assertEqual(expected_primary_metadata, metadata_primary)
        self.assertIsNone(
            self.metadata_manager.get_job_completion_time_for_args(job_args_primary)
        )

        # Assert Secondary
        metadata_secondary = self.metadata_manager_secondary.get_metadata_for_job_args(
            job_args_secondary
        )
        self.assertEqual(expected_secondary_metadata, metadata_secondary)
        self.assertIsNone(
            self.metadata_manager_secondary.get_job_completion_time_for_args(
                job_args_secondary
            )
        )

        # Assert
        with freeze_time("2015-01-02T03:06:06"):
            same_instance = (
                r"Destination instance should not have any valid metadata rows."
            )
            with self.assertRaisesRegex(ValueError, same_instance):
                self.metadata_manager_secondary.transfer_metadata_to_new_instance(
                    self.metadata_manager
                )

        # Assert
        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            metadata = (
                session.query(schema.DirectIngestViewMaterializationMetadata)
                .filter_by(
                    region_code=self.metadata_manager_secondary.region_code.upper(),
                    instance=self.metadata_manager_secondary.ingest_instance.value,
                )
                .one()
            )
            # Check here that origin database is same
            self.assertEqual(
                expected_secondary_metadata, convert_schema_object_to_entity(metadata)
            )

    def test_get_instance_summary(self) -> None:
        self.assertEqual({}, self.metadata_manager.get_instance_summaries())
        self.assertEqual({}, self.metadata_manager_secondary.get_instance_summaries())

        view_1_args = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
        )
        view_1_args_2 = IngestViewMaterializationArgs(
            ingest_view_name="ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=datetime.datetime(2015, 1, 2, 2, 2, 2, 2),
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 3, 3, 3, 3, 3),
        )
        view_2_args = IngestViewMaterializationArgs(
            ingest_view_name="other_ingest_view_name",
            ingest_instance=self.metadata_manager.ingest_instance,
            lower_bound_datetime_exclusive=None,
            upper_bound_datetime_inclusive=datetime.datetime(2015, 1, 4, 4, 4, 4, 4),
        )

        with freeze_time("2015-01-02T03:05:05"):
            for args in [view_1_args, view_1_args_2, view_2_args]:
                self.metadata_manager.register_ingest_materialization_job(args)

        self.assertEqual(
            {
                "ingest_view_name": IngestViewMaterializationSummary(
                    ingest_view_name="ingest_view_name",
                    num_pending_jobs=2,
                    num_completed_jobs=0,
                    completed_jobs_max_datetime=None,
                    pending_jobs_min_datetime=view_1_args.upper_bound_datetime_inclusive,
                ),
                "other_ingest_view_name": IngestViewMaterializationSummary(
                    ingest_view_name="other_ingest_view_name",
                    num_pending_jobs=1,
                    num_completed_jobs=0,
                    completed_jobs_max_datetime=None,
                    pending_jobs_min_datetime=view_2_args.upper_bound_datetime_inclusive,
                ),
            },
            self.metadata_manager.get_instance_summaries(),
        )
        self.assertEqual({}, self.metadata_manager_secondary.get_instance_summaries())

        with freeze_time("2015-01-02T03:06:06"):
            for args in [view_1_args, view_2_args]:
                self.metadata_manager.mark_ingest_view_materialized(args)

        self.assertEqual(
            {
                "ingest_view_name": IngestViewMaterializationSummary(
                    ingest_view_name="ingest_view_name",
                    num_pending_jobs=1,
                    num_completed_jobs=1,
                    completed_jobs_max_datetime=view_1_args.upper_bound_datetime_inclusive,
                    pending_jobs_min_datetime=view_1_args_2.upper_bound_datetime_inclusive,
                ),
                "other_ingest_view_name": IngestViewMaterializationSummary(
                    ingest_view_name="other_ingest_view_name",
                    num_pending_jobs=0,
                    num_completed_jobs=1,
                    completed_jobs_max_datetime=view_2_args.upper_bound_datetime_inclusive,
                    pending_jobs_min_datetime=None,
                ),
            },
            self.metadata_manager.get_instance_summaries(),
        )
        self.assertEqual({}, self.metadata_manager_secondary.get_instance_summaries())
