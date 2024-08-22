"""
Snapshots for recidiviz/tests/tools/auth/prep_roster_sync_test.py
Update snapshots automatically by running `pytest recidiviz/tests/tools/auth/prep_roster_sync_test.py --snapshot-update
Remember to include a docstring like this after updating the snapshots for Pylint purposes
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot

snapshots = Snapshot()

snapshots[
    "CleanupUserOverridesTest.CleanupUserOverridesTest test_add_user_overrides"
] = [
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user1@testdomain.com, external_id=1234, roles=['leadership_role'], district=District 7, first_name=Test, last_name=User, blocked=True)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user2@testdomain.com, external_id=None, roles=['leadership_role'], district=None, first_name=None, last_name=None, blocked=False)"
    ),
]

snapshots["CleanupUserOverridesTest.CleanupUserOverridesTest test_full_roster"] = [
    GenericRepr(
        "Roster(state_code=US_XX, email_address=recently_logged_in_user@testdomain.com, external_id=123, roles=['supervision_officer'], district=D1, first_name=Test, last_name=User)"
    )
]

snapshots[
    "CleanupUserOverridesTest.CleanupUserOverridesTest test_full_user_override"
] = [
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=user_in_sync_query@testdomain.com, external_id=234, roles=['supervision_officer'], district=D2, first_name=Test, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=recently_created_user@testdomain.com, external_id=345, roles=['supervision_officer'], district=D3, first_name=Test, last_name=User, blocked=False)"
    ),
    GenericRepr(
        "UserOverride(state_code=US_XX, email_address=recently_logged_in_user@testdomain.com, external_id=123, roles=['supervision_officer', 'custom_role'], district=D2, first_name=Test, last_name=User, blocked=False)"
    ),
]
