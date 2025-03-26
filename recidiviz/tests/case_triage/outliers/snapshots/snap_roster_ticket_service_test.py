"""	
Snapshots for recidiviz/tests/case_triage/outliers/snapshots/snap_roster_ticket_service_test.py
Update snapshots automatically by running `pytest recidiviz/tests/admin_panel/routes/outliers_test.py --snapshot-update	
Remember to include a docstring like this after updating the snapshots for Pylint purposes	
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots[
    "test_build_ticket_description[add_multiple_officer] 1"
] = """Requesting Person has requested that the following officer(s) be added to the caseload of Alice Supervisor:
- Jane Officer, District A (supervised by Alice Supervisor, Bob Supervisor)
- John Officer, District B (supervised by Bob Supervisor)

Other staff affected by this change:
- Alice Supervisor
- Bob Supervisor

Note from user:
Add these officers to the caseload.It's imperative that they're added.
Please speak with admin about this."""

snapshots[
    "test_build_ticket_description[add_single_officer] 1"
] = """Requesting Person has requested that the following officer(s) be added to the caseload of Alice Supervisor:
- Jane Officer, District A (supervised by Alice Supervisor, Bob Supervisor)

Other staff affected by this change:
- Alice Supervisor
- Bob Supervisor

Note from user:
Add this officer, please."""

snapshots[
    "test_build_ticket_description[remove_multiple_officers] 1"
] = """Requesting Person has requested that the following officer(s) be removed from the caseload of Alice Supervisor:
- Jane Officer, District A (supervised by Alice Supervisor, Bob Supervisor)
- John Officer, District B (supervised by Bob Supervisor)

Other staff affected by this change:
- Alice Supervisor
- Bob Supervisor

Note from user:
Remove these officers from my caseloads."""

snapshots[
    "test_build_ticket_description[remove_single_officer] 1"
] = """Requesting Person has requested that the following officer(s) be removed from the caseload of Alice Supervisor:
- Jane Officer, District A (supervised by Alice Supervisor, Bob Supervisor)

Other staff affected by this change:
- Alice Supervisor
- Bob Supervisor

Note from user:
Remove this officer from my caseload.
Thank you!"""
