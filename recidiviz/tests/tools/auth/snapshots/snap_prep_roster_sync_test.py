"""
Snapshots for recidiviz/tests/tools/auth/prep_roster_sync_test.py
Update snapshots automatically by running `pytest recidiviz/tests/tools/auth/prep_roster_sync_test.py --snapshot-update`
Remember to include a docstring like this after updating the snapshots for Pylint purposes
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot

snapshots = Snapshot()

snapshots["PrepRosterSyncTest.PrepRosterSyncTest test_add_user_overrides"] = [
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user1@testdomain.com, external_id=1234, roles=['leadership_role'], district=District 7, first_name=Test, last_name=User, blocked=True)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user2@testdomain.com, external_id=None, roles=['leadership_role'], district=None, first_name=None, last_name=None, blocked=False)"
    ),
]

snapshots["PrepRosterSyncTest.PrepRosterSyncTest test_find_and_handle_diffs"] = [
    (
        GenericRepr(
            "UserOverride(state_code=US_XX, email_address=user_with_equivalent_role@testdomain.com, external_id=789, roles=['supervision_staff', 'tt group'], district=D7, first_name=Test, last_name=User, blocked=False)"
        ),
        {
            "email_address": "user_with_equivalent_role@testdomain.com",
            "roles": ["supervision_line_staff", "tt group"],
            "state_code": "US_XX",
            "user_hash": "t7AYschQxi0piP/4J63k8bXwjmR4QLjCIT9lBAdSGf0=",
        },
    ),
    (
        GenericRepr(
            "UserOverride(state_code=US_XX, email_address=user_with_different_district@testdomain.com, external_id=901, roles=['supervision_staff'], district=D9, first_name=Test, last_name=User, blocked=False)"
        ),
        {
            "district": "changed district",
            "email_address": "user_with_different_district@testdomain.com",
            "roles": ["supervision_staff"],
            "state_code": "US_XX",
            "user_hash": "SC00iF1/9zEtxqivkgXMaxoBD2ZyzfIaEo+Fha/FHLg=",
        },
    ),
    (
        GenericRepr(
            "UserOverride(state_code=US_XX, email_address=user_with_different_role@testdomain.com, external_id=890, roles=['leadership_role', 'tt group'], district=D8, first_name=Test, last_name=User, blocked=False)"
        ),
        {
            "email_address": "user_with_different_role@testdomain.com",
            "roles": ["leadership_role", "supervision_line_staff", "tt group"],
            "state_code": "US_XX",
            "user_hash": "yeZtA6vdFehLFnYeilVLnRSD5DK/ysrHGRcvNcn3/N8=",
        },
    ),
    (
        GenericRepr(
            "UserOverride(state_code=US_XX, email_address=user_in_sync_query@testdomain.com, external_id=234, roles=['supervision_staff'], district=D2, first_name=Test, last_name=User, blocked=False)"
        ),
        {
            "email_address": "user_in_sync_query@testdomain.com",
            "roles": ["supervision_staff"],
            "state_code": "US_XX",
            "user_hash": "rFOFFPbGGwRWUbHa0ViwNmjGAEgW+lngeSMln3lKSYw=",
        },
    ),
]

snapshots["PrepRosterSyncTest.PrepRosterSyncTest test_full_roster"] = [
    GenericRepr(
        "Roster(state_code=US_XX, email_address=recently_logged_in_user@testdomain.com, external_id=123, roles=['supervision_line_staff'], district=D1, first_name=Test, last_name=User)"
    ),
    GenericRepr(
        "Roster(state_code=US_XX, email_address=user_to_keep_unchanged@testdomain.com, external_id=1234, roles=['supervision_staff'], district=D12, first_name=Test, last_name=User)"
    ),
]

snapshots["PrepRosterSyncTest.PrepRosterSyncTest test_full_user_override"] = [
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=recently_created_user@testdomain.com, external_id=345, roles=['supervision_line_staff'], district=D3, first_name=Test, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=recently_logged_in_user@testdomain.com, external_id=123, roles=['supervision_line_staff', 'custom_role'], district=D2, first_name=Test, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user_to_keep_unchanged@testdomain.com, external_id=1234, roles=['supervision_staff'], district=D12, first_name=Test, last_name=User, blocked=None)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user_with_multiple_diffs@testdomain.com, external_id=012, roles=['supervision_line_staff'], district=changed district, first_name=changed name, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user_with_equivalent_role@testdomain.com, external_id=789, roles=['supervision_line_staff', 'tt group'], district=D7, first_name=Test, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user_with_different_district@testdomain.com, external_id=901, roles=['supervision_staff'], district=changed district, first_name=Test, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user_with_different_role@testdomain.com, external_id=890, roles=['leadership_role', 'supervision_line_staff', 'tt group'], district=D8, first_name=Test, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user_in_sync_query@testdomain.com, external_id=234, roles=['leadership_role', 'supervision_staff'], district=D2, first_name=Test, last_name=User, blocked=False)"
    ),
]
