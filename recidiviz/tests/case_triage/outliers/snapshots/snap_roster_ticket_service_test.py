"""	
Snapshots for recidiviz/tests/case_triage/outliers/snapshots/snap_roster_ticket_service_test.py
Update snapshots automatically by running `pytest recidiviz/tests/admin_panel/routes/outliers_test.py --snapshot-update	
Remember to include a docstring like this after updating the snapshots for Pylint purposes	
"""

from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["test_request_roster_change[add_multiple_officer] 1"] = {
    "contacts": [{"email": "requester@example.com"}],
    "ticket_attributes": {
        "_default_description_": """Test Requester has requested that the following officer(s) be added to the caseload of Alice Supervisor:
- Jane Officer <jane@testdomain.com>, District A (supervised by Alice Supervisor, Bob Supervisor)
- John Officer <email not found>, District B (supervised by Bob Supervisor)
- Sam Officer <sam@testdomain.com>, District C (no supervisors listed)

Other supervisor(s) affected by this change:
- Alice Supervisor <alice@testdomain.com>
- Bob Supervisor <email not found>

Note from user:
Add these officers to the caseload.It's imperative that they're added.
Please speak with admin about this.""",
        "_default_title_": "Team Addition Request Submitted",
    },
    "ticket_type_id": 1,
}

snapshots["test_request_roster_change[add_multiple_officers_test] 1"] = {
    "contacts": [{"email": "requester@example.com"}],
    "ticket_attributes": {
        "_default_description_": """PLEASE DISREGARD. THIS IS A TEST REQUEST.
Test Requester has requested that the following officer(s) be added to the caseload of Alice Supervisor:
- Jane Officer <jane@testdomain.com>, District A (supervised by Alice Supervisor, Bob Supervisor)
- John Officer <email not found>, District B (supervised by Bob Supervisor)

Other supervisor(s) affected by this change:
- Alice Supervisor <alice@testdomain.com>
- Bob Supervisor <email not found>

Note from user:
From staging, this request is to add these officers to my caseloads""",
        "_default_title_": "[TEST] Team Addition Request Submitted",
    },
    "ticket_type_id": 1,
}

snapshots["test_request_roster_change[add_single_officer] 1"] = {
    "contacts": [{"email": "requester@example.com"}],
    "ticket_attributes": {
        "_default_description_": """Test Requester has requested that the following officer(s) be added to the caseload of Alice Supervisor:
- Jane Officer <jane@testdomain.com>, District A (supervised by Alice Supervisor, Bob Supervisor)

Other supervisor(s) affected by this change:
- Alice Supervisor <alice@testdomain.com>
- Bob Supervisor <email not found>

Note from user:
Add this officer, please.""",
        "_default_title_": "Team Addition Request Submitted",
    },
    "ticket_type_id": 1,
}

snapshots["test_request_roster_change[remove_multiple_officers] 1"] = {
    "contacts": [{"email": "requester@example.com"}],
    "ticket_attributes": {
        "_default_description_": """Test Requester has requested that the following officer(s) be removed from the caseload of Alice Supervisor:
- Jane Officer <jane@testdomain.com>, District A
- John Officer <email not found>, District B

Other supervisor(s) affected by this change:
- Alice Supervisor <alice@testdomain.com>
- Bob Supervisor <email not found>

Note from user:
Remove these officers from my caseloads.""",
        "_default_title_": "Team Removal Request Submitted",
    },
    "ticket_type_id": 1,
}

snapshots["test_request_roster_change[remove_multiple_officers_test] 1"] = {
    "contacts": [{"email": "requester@example.com"}],
    "ticket_attributes": {
        "_default_description_": """PLEASE DISREGARD. THIS IS A TEST REQUEST.
Test Requester has requested that the following officer(s) be removed from the caseload of Alice Supervisor:
- Jane Officer <jane@testdomain.com>, District A
- John Officer <email not found>, District B

Other supervisor(s) affected by this change:
- Alice Supervisor <alice@testdomain.com>
- Bob Supervisor <email not found>

Note from user:
As a recidiviz user, I'm asking to remove these officers from my caseloads""",
        "_default_title_": "[TEST] Team Removal Request Submitted",
    },
    "ticket_type_id": 1,
}

snapshots["test_request_roster_change[remove_single_officer] 1"] = {
    "contacts": [{"email": "requester@example.com"}],
    "ticket_attributes": {
        "_default_description_": """Test Requester has requested that the following officer(s) be removed from the caseload of Alice Supervisor:
- Jane Officer <jane@testdomain.com>, District A

Other supervisor(s) affected by this change:
- Alice Supervisor <alice@testdomain.com>
- Bob Supervisor <email not found>

Note from user:
Remove this officer from my caseload.
Thank you!""",
        "_default_title_": "Team Removal Request Submitted",
    },
    "ticket_type_id": 1,
}
