"""
Snapshots for recidiviz/tests/auth/auth_users_endpoint_test.py
Update snapshots automatically by running `pytest recidiviz/tests/auth/auth_users_endpoint_test.py --snapshot-update`
Remember to include a docstring like this after updating the snapshots for Pylint purposes
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots["AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_add_user"] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "District",
        "emailAddress": "add_user@domain.org",
        "externalId": "ABC",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "0D1WiekUDUBhjVnqyNbbwGJP2xll0CS9vfsnPrxnmSE=",
    },
    {
        "allowedSupervisionLocationIds": "1, 2",
        "allowedSupervisionLocationLevel": "level_1_supervision_location",
        "blocked": False,
        "district": "1, 2",
        "emailAddress": "parameter@domain.org",
        "externalId": None,
        "featureVariants": {"D": "E"},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {"A": True, "B": False},
        "stateCode": "US_MO",
        "userHash": "flf+tuxZFuMOTgZf8aIZiDj/a4Cw4tIwRl7WcpVdCA0=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_add_user_repeat_email"
] = {
    "district": "D1",
    "emailAddress": "parameter@domain.org",
    "externalId": "XYZ",
    "firstName": "Test",
    "lastName": "User",
    "pseudonymizedId": "pseudo-XYZ",
    "role": "leadership_role",
    "stateCode": "US_ID",
    "userHash": "flf+tuxZFuMOTgZf8aIZiDj/a4Cw4tIwRl7WcpVdCA0=",
}

snapshots["AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_user"] = {
    "allowedSupervisionLocationIds": "",
    "allowedSupervisionLocationLevel": "",
    "blocked": False,
    "district": "District",
    "emailAddress": "parameter@domain.org",
    "externalId": "ABC",
    "featureVariants": {"D": "E"},
    "firstName": None,
    "lastName": None,
    "pseudonymizedId": "pseudo-ABC",
    "role": "leadership_role",
    "routes": {"A": True, "B": False},
    "stateCode": "US_CO",
    "userHash": "flf+tuxZFuMOTgZf8aIZiDj/a4Cw4tIwRl7WcpVdCA0=",
}

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_users_no_permissions"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "District 4",
        "emailAddress": "leadership@domain.org",
        "externalId": "12345",
        "featureVariants": {},
        "firstName": "Test A.",
        "lastName": "User",
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_users_some_overrides"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": True,
        "district": "D1",
        "emailAddress": "leadership@domain.org",
        "externalId": "user_1_override.external_id",
        "featureVariants": {"C": True, "new variant": False},
        "firstName": "Fake",
        "lastName": "User",
        "pseudonymizedId": "hashed-user_1_override",
        "role": "user_1_override.role",
        "routes": {"overridden route": True},
        "stateCode": "US_ND",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "D3",
        "emailAddress": "supervision_staff@domain.org",
        "externalId": "abc",
        "featureVariants": {},
        "firstName": "John",
        "lastName": "Doe",
        "pseudonymizedId": "pseudo-abc",
        "role": "supervision_staff",
        "routes": {},
        "stateCode": "US_ID",
        "userHash": "EghmFPYcNI/RKWs9Cdt3P5nvGFhwM/uSkKKY1xVibvI=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_users_with_empty_overrides"
] = [
    {
        "allowedSupervisionLocationIds": "4, 10A",
        "allowedSupervisionLocationLevel": "level_1_supervision_location",
        "blocked": False,
        "district": "4, 10A",
        "emailAddress": "leadership@domain.org",
        "externalId": "12345",
        "featureVariants": {},
        "firstName": "Test A.",
        "lastName": "User",
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {"A": True},
        "stateCode": "US_MO",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_users_with_null_values"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": True,
        "district": None,
        "emailAddress": "leadership@domain.org",
        "externalId": "A1B2",
        "featureVariants": {"C": True},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {"A": True, "B": True, "C": False},
        "stateCode": "US_ME",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]

snapshots["AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_update_users"] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "D1",
        "emailAddress": "parameter@domain.org",
        "externalId": "123",
        "featureVariants": {},
        "firstName": "Test",
        "lastName": "User",
        "pseudonymizedId": "pseudo-123",
        "role": "supervision_staff",
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "flf+tuxZFuMOTgZf8aIZiDj/a4Cw4tIwRl7WcpVdCA0=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "user@domain.org",
        "externalId": "456",
        "featureVariants": {},
        "firstName": "Original",
        "lastName": "Name",
        "pseudonymizedId": None,
        "role": "supervision_staff",
        "routes": {},
        "stateCode": "US_TN",
        "userHash": "j8+pC9rc353XWt4x1fg+3Km9TQtr5XMZMT8Frl37H/o=",
    },
]

snapshots["AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster"] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@domain.org",
        "externalId": "3975",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3975",
        "role": "leadership_role",
        "routes": {"A": True},
        "stateCode": "US_XX",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "supervision_staff@domain.org",
        "externalId": "3706",
        "featureVariants": {},
        "firstName": "supervision",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3706",
        "role": "supervision_staff",
        "routes": {"B": True},
        "stateCode": "US_XX",
        "userHash": "EghmFPYcNI/RKWs9Cdt3P5nvGFhwM/uSkKKY1xVibvI=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_missing_external_id"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "NEW DISTRICT",
        "emailAddress": "leadership@domain.org",
        "externalId": "1234",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {"A": True},
        "stateCode": "US_XX",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_update_user"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@domain.org",
        "externalId": "3975",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3975",
        "role": "leadership_role",
        "routes": {"A": True},
        "stateCode": "US_XX",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "supervision_staff@domain.org",
        "externalId": None,
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "role": "supervision_staff",
        "routes": {"B": True},
        "stateCode": "US_XX",
        "userHash": "EghmFPYcNI/RKWs9Cdt3P5nvGFhwM/uSkKKY1xVibvI=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_update_user_with_override"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@domain.org",
        "externalId": "3975",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3975",
        "role": "leadership_role",
        "routes": {"A": True},
        "stateCode": "US_XX",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_with_malformed_email_address"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@domain.org",
        "externalId": "0000",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {},
        "stateCode": "US_XX",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_with_missing_associated_role"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@domain.org",
        "externalId": "0000",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {},
        "stateCode": "US_XX",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_with_missing_email_address"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@domain.org",
        "externalId": "0000",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "role": "leadership_role",
        "routes": {},
        "stateCode": "US_XX",
        "userHash": "qKTCaVmWmjqbJX0SckE082QJKv6sE4W/bKzfHQZJNYk=",
    }
]