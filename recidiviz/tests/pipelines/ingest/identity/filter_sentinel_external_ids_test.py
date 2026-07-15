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
"""Tests for FilterSentinelExternalIds."""
import datetime
import logging
import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
    IdentityExternalId,
    IdentityFragment,
    IdentityName,
)
from recidiviz.pipelines.ingest.identity import filter_sentinel_external_ids
from recidiviz.pipelines.ingest.identity.filter_sentinel_external_ids import (
    FilterSentinelExternalIds,
)
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    IdentityIngestPipelineConfig,
    IdentityIngestPipelineTenantConfig,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

_TENANT = Tenant.US_XX
_VIEW_NAME = "eg_person"

_UPPER_BOUND_TS = datetime.datetime(2024, 1, 15).timestamp()
_LATER_UPPER_BOUND_TS = datetime.datetime(2024, 2, 15).timestamp()

_SID_TYPE = "US_XX_SID"
_DOC_TYPE = "US_XX_DOC"
_BOOKING_TYPE = "US_XX_BOOKING"

_SENTINEL_EID = "000000"

_DEFAULT_ONLY_CONFIG = IdentityIngestPipelineConfig(
    default_config=IdentityIngestPipelineTenantConfig(),
    tenant_configs={},
)

_CONFIG_WITH_BOOKING_OVERRIDE = IdentityIngestPipelineConfig(
    default_config=IdentityIngestPipelineTenantConfig(),
    tenant_configs={
        (_TENANT, PersonType.JII): IdentityIngestPipelineTenantConfig(
            max_ids_per_type_overrides={_BOOKING_TYPE: 3}
        )
    },
)


def _make_attrs(given_name: str) -> IdentityAttributes:
    return IdentityAttributes(
        tenant=_TENANT,
        person_type=PersonType.JII,
        name=IdentityName(tenant=_TENANT, given_name=given_name),
    )


def _make_fragment(
    external_ids: list[tuple[str, str]],
    attrs: IdentityAttributes | None,
) -> IdentityFragment:
    return IdentityFragment(
        tenant=_TENANT,
        external_ids=[
            IdentityExternalId(tenant=_TENANT, external_id=eid, id_type=etype)
            for eid, etype in external_ids
        ],
        attributes=attrs,
    )


def _make_transform(
    pipeline_config: IdentityIngestPipelineConfig,
    person_type: PersonType = PersonType.JII,
) -> FilterSentinelExternalIds:
    return FilterSentinelExternalIds(
        ingest_view_name=_VIEW_NAME,
        tenant=_TENANT,
        person_type=person_type,
        pipeline_config=pipeline_config,
    )


class TestFilterSentinelExternalIds(unittest.TestCase):
    """Tests for FilterSentinelExternalIds."""

    def test_sentinel_stripped_from_multi_eid_fragments(self) -> None:
        """A sentinel external ID shared by two fragments (exceeding the
        default max of 1) is stripped from both, but the fragments are kept
        with their remaining external IDs and attributes."""
        frag1 = _make_fragment(
            [(_SENTINEL_EID, _DOC_TYPE), ("S1", _SID_TYPE)], _make_attrs("DOROTHY")
        )
        frag2 = _make_fragment(
            [(_SENTINEL_EID, _DOC_TYPE), ("S2", _SID_TYPE)], _make_attrs("TOTO")
        )

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(_UPPER_BOUND_TS, frag1), (_UPPER_BOUND_TS, frag2)])
                | _make_transform(_DEFAULT_ONLY_CONFIG)
            )
            assert_that(
                result,
                equal_to(
                    [
                        (
                            _UPPER_BOUND_TS,
                            _make_fragment([("S1", _SID_TYPE)], _make_attrs("DOROTHY")),
                        ),
                        (
                            _UPPER_BOUND_TS,
                            _make_fragment([("S2", _SID_TYPE)], _make_attrs("TOTO")),
                        ),
                    ]
                ),
            )

    def test_fragment_with_only_sentinel_eid_dropped(self) -> None:
        """A fragment whose only external ID is a sentinel is dropped entirely,
        while another fragment carrying the sentinel keeps its other IDs."""
        only_sentinel_frag = _make_fragment(
            [(_SENTINEL_EID, _DOC_TYPE)], _make_attrs("DOROTHY")
        )
        multi_eid_frag = _make_fragment(
            [(_SENTINEL_EID, _DOC_TYPE), ("S2", _SID_TYPE)], _make_attrs("TOTO")
        )

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        (_UPPER_BOUND_TS, only_sentinel_frag),
                        (_UPPER_BOUND_TS, multi_eid_frag),
                    ]
                )
                | _make_transform(_DEFAULT_ONLY_CONFIG)
            )
            assert_that(
                result,
                equal_to(
                    [
                        (
                            _UPPER_BOUND_TS,
                            _make_fragment([("S2", _SID_TYPE)], _make_attrs("TOTO")),
                        ),
                    ]
                ),
            )

    def test_non_sentinel_eids_unaffected(self) -> None:
        """Fragments whose external IDs each appear on only one fragment pass
        through unchanged."""
        frag1 = _make_fragment(
            [("D1", _DOC_TYPE), ("S1", _SID_TYPE)], _make_attrs("DOROTHY")
        )
        frag2 = _make_fragment([("S2", _SID_TYPE)], _make_attrs("TOTO"))

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(_UPPER_BOUND_TS, frag1), (_UPPER_BOUND_TS, frag2)])
                | _make_transform(_DEFAULT_ONLY_CONFIG)
            )
            assert_that(
                result,
                equal_to([(_UPPER_BOUND_TS, frag1), (_UPPER_BOUND_TS, frag2)]),
            )

    def test_logs_excluded_sentinels(self) -> None:
        """Each excluded sentinel is logged with its external_id, id_type, and
        fragment count."""
        frag1 = _make_fragment(
            [(_SENTINEL_EID, _DOC_TYPE), ("S1", _SID_TYPE)], _make_attrs("DOROTHY")
        )
        frag2 = _make_fragment(
            [(_SENTINEL_EID, _DOC_TYPE), ("S2", _SID_TYPE)], _make_attrs("TOTO")
        )

        with self.assertLogs(level=logging.WARNING) as log_capture:
            with create_test_pipeline() as p:
                _ = (
                    p
                    | beam.Create([(_UPPER_BOUND_TS, frag1), (_UPPER_BOUND_TS, frag2)])
                    | _make_transform(_DEFAULT_ONLY_CONFIG)
                )

        sentinel_logs = [
            message
            for message in log_capture.output
            if "Excluding sentinel external id" in message
        ]
        self.assertEqual(1, len(sentinel_logs))
        self.assertIn(
            f"external_id=[{_SENTINEL_EID}], id_type=[{_DOC_TYPE}], "
            f"fragment_count=[2]",
            sentinel_logs[0],
        )

    def test_at_threshold_kept_above_threshold_stripped(self) -> None:
        """With a configured max of 3 for the booking ID type, a booking ID on
        exactly 3 fragments is kept while a booking ID on 4 fragments is
        treated as a sentinel and stripped."""
        at_threshold_frags = [
            _make_fragment(
                [("B1", _BOOKING_TYPE), (f"S{i}", _SID_TYPE)],
                _make_attrs("A" * (i + 1)),
            )
            for i in range(3)
        ]
        above_threshold_frags = [
            _make_fragment(
                [("B2", _BOOKING_TYPE), (f"T{i}", _SID_TYPE)],
                _make_attrs("B" * (i + 1)),
            )
            for i in range(4)
        ]

        expected_stripped_frags = [
            _make_fragment([(f"T{i}", _SID_TYPE)], _make_attrs("B" * (i + 1)))
            for i in range(4)
        ]

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        (_UPPER_BOUND_TS, frag)
                        for frag in at_threshold_frags + above_threshold_frags
                    ]
                )
                | _make_transform(_CONFIG_WITH_BOOKING_OVERRIDE)
            )
            assert_that(
                result,
                equal_to(
                    [(_UPPER_BOUND_TS, frag) for frag in at_threshold_frags]
                    + [(_UPPER_BOUND_TS, frag) for frag in expected_stripped_frags]
                ),
            )

    def test_no_override_config_uses_default_of_one(self) -> None:
        """With no overrides configured, an external ID on two fragments is a
        sentinel while an external ID on one fragment is kept."""
        frag1 = _make_fragment(
            [("B1", _BOOKING_TYPE), ("S1", _SID_TYPE)], _make_attrs("DOROTHY")
        )
        frag2 = _make_fragment(
            [("B1", _BOOKING_TYPE), ("S2", _SID_TYPE)], _make_attrs("TOTO")
        )

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create([(_UPPER_BOUND_TS, frag1), (_UPPER_BOUND_TS, frag2)])
                | _make_transform(_DEFAULT_ONLY_CONFIG)
            )
            assert_that(
                result,
                equal_to(
                    [
                        (
                            _UPPER_BOUND_TS,
                            _make_fragment([("S1", _SID_TYPE)], _make_attrs("DOROTHY")),
                        ),
                        (
                            _UPPER_BOUND_TS,
                            _make_fragment([("S2", _SID_TYPE)], _make_attrs("TOTO")),
                        ),
                    ]
                ),
            )

    def test_too_many_sentinels_fails_pipeline(self) -> None:
        """If the number of sentinel external id keys found exceeds the
        maximum, the pipeline fails loudly rather than silently stripping a
        huge number of external IDs (which more likely signals a misconfigured
        threshold)."""
        # Three distinct sentinel EIDs, each shared by two fragments (exceeding
        # the default max of 1), yielding three sentinel keys.
        fragments = [
            _make_fragment(
                [
                    (f"SENTINEL{sentinel_i}", _DOC_TYPE),
                    (f"S{sentinel_i}_{frag_i}", _SID_TYPE),
                ],
                _make_attrs("DOROTHY"),
            )
            for sentinel_i in range(3)
            for frag_i in range(2)
        ]

        with mock.patch.object(
            filter_sentinel_external_ids, "MAX_SENTINEL_EXTERNAL_IDS_PER_VIEW", 2
        ):
            with self.assertRaisesRegex(Exception, r"exceeds the maximum of \[2\]"):
                with create_test_pipeline() as p:
                    _ = (
                        p
                        | beam.Create([(_UPPER_BOUND_TS, frag) for frag in fragments])
                        | _make_transform(_DEFAULT_ONLY_CONFIG)
                    )

    def test_same_external_id_across_dates_not_sentinel(self) -> None:
        """Fragment counts are computed per upper-bound date, so the same
        external ID appearing once per date (the same person present in
        multiple snapshots) is not a sentinel."""
        frag_date_1 = _make_fragment([("S1", _SID_TYPE)], _make_attrs("DOROTHY"))
        frag_date_2 = _make_fragment([("S1", _SID_TYPE)], _make_attrs("DOROTHY"))

        with create_test_pipeline() as p:
            result = (
                p
                | beam.Create(
                    [
                        (_UPPER_BOUND_TS, frag_date_1),
                        (_LATER_UPPER_BOUND_TS, frag_date_2),
                    ]
                )
                | _make_transform(_DEFAULT_ONLY_CONFIG)
            )
            assert_that(
                result,
                equal_to(
                    [
                        (_UPPER_BOUND_TS, frag_date_1),
                        (_LATER_UPPER_BOUND_TS, frag_date_2),
                    ]
                ),
            )
