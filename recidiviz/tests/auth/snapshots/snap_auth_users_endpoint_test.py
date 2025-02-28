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
        "emailAddress": "add_user@testdomain.com",
        "externalId": "ABC",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "yRfBcQIOiTFhlzX/Erh5NLDygGJxoGUlTm7iVPyC5CY=",
    },
    {
        "allowedSupervisionLocationIds": "1, 2",
        "allowedSupervisionLocationLevel": "level_1_supervision_location",
        "blocked": False,
        "district": "1, 2",
        "emailAddress": "parameter@testdomain.com",
        "externalId": None,
        "featureVariants": {"D": {}},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {"A": True, "B": False},
        "stateCode": "US_MO",
        "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_add_user_multiple_roles"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "District",
        "emailAddress": "add_user@testdomain.com",
        "externalId": "ABC",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "yRfBcQIOiTFhlzX/Erh5NLDygGJxoGUlTm7iVPyC5CY=",
    },
    {
        "allowedSupervisionLocationIds": "1, 2",
        "allowedSupervisionLocationLevel": "level_1_supervision_location",
        "blocked": False,
        "district": "1, 2",
        "emailAddress": "parameter@testdomain.com",
        "externalId": None,
        "featureVariants": {"feature1": {}},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role", "supervision_staff"],
        "routes": {"A": True, "B": True},
        "stateCode": "US_MO",
        "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_add_user_repeat_email"
] = {
    "blocked": False,
    "district": "D1",
    "emailAddress": "parameter@testdomain.com",
    "externalId": "XYZ",
    "firstName": "Test",
    "lastName": "User",
    "pseudonymizedId": "pseudo-XYZ",
    "roles": ["leadership_role"],
    "stateCode": "US_ID",
    "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
}

snapshots["AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_user"] = {
    "allowedSupervisionLocationIds": "",
    "allowedSupervisionLocationLevel": "",
    "blocked": False,
    "district": "District",
    "emailAddress": "parameter@testdomain.com",
    "externalId": "ABC",
    "featureVariants": {"D": {}},
    "firstName": None,
    "lastName": None,
    "pseudonymizedId": "pseudo-ABC",
    "roles": ["leadership_role"],
    "routes": {"A": True, "B": False},
    "stateCode": "US_CO",
    "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
}

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_users_no_permissions"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "District 4",
        "emailAddress": "leadership@testdomain.com",
        "externalId": "12345",
        "featureVariants": {},
        "firstName": "Test A.",
        "lastName": "User",
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
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
        "emailAddress": "leadership@testdomain.com",
        "externalId": "user_1_override.external_id",
        "featureVariants": {"C": {}, "new variant": False},
        "firstName": "Fake",
        "lastName": "User",
        "pseudonymizedId": "hashed-user_1_override",
        "roles": ["user_1_override.role"],
        "routes": {"overridden route": True},
        "stateCode": "US_ND",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "D3",
        "emailAddress": "supervision_staff@testdomain.com",
        "externalId": "abc",
        "featureVariants": {},
        "firstName": "John",
        "lastName": "Doe",
        "pseudonymizedId": "pseudo-abc",
        "roles": ["supervision_staff"],
        "routes": {},
        "stateCode": "US_ID",
        "userHash": "_uYmjI0oMriD8yRXsTt1quVrTkZZuRHJ35X+szGMHJQ=",
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
        "emailAddress": "leadership@testdomain.com",
        "externalId": "12345",
        "featureVariants": {},
        "firstName": "Test A.",
        "lastName": "User",
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {"A": True},
        "stateCode": "US_MO",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_users_with_multiple_roles_no_conflicts"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "leadership@testdomain.com",
        "externalId": None,
        "featureVariants": {"feature1": {}},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role", "supervision_staff"],
        "routes": {"A": True, "B": True},
        "stateCode": "US_CO",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_get_users_with_multiple_roles_with_conflicts"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "leadership@testdomain.com",
        "externalId": None,
        "featureVariants": {
            "feature1": {"activeDate": "2024-04-30T14:45:09.865Z"},
            "feature2": {},
            "feature3": {"activeDate": "2024-04-30T14:45:09.865Z"},
        },
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role", "supervision_staff"],
        "routes": {"A": True, "B": True},
        "stateCode": "US_CO",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
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
        "emailAddress": "leadership@testdomain.com",
        "externalId": "A1B2",
        "featureVariants": {"C": {}},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {"A": True, "B": True, "C": False},
        "stateCode": "US_ME",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_update_user_add_role"
] = {
    "allowedSupervisionLocationIds": "",
    "allowedSupervisionLocationLevel": "",
    "blocked": False,
    "district": None,
    "emailAddress": "parameter@testdomain.com",
    "externalId": None,
    "featureVariants": {"feature1": {}, "feature2": {}},
    "firstName": None,
    "lastName": None,
    "pseudonymizedId": None,
    "roles": ["supervision_staff", "leadership_role"],
    "routes": {"A": True, "B": True},
    "stateCode": "US_CO",
    "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
}

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_update_user_in_roster"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "D1",
        "emailAddress": "parameter@testdomain.com",
        "externalId": "123",
        "featureVariants": {},
        "firstName": "Test",
        "lastName": "User",
        "pseudonymizedId": "pseudo-123",
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_update_user_in_user_override"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "parameter@testdomain.com",
        "externalId": "UPDATED ID",
        "featureVariants": {},
        "firstName": "Updated",
        "lastName": "Name",
        "pseudonymizedId": "pseudo-UPDATED ID",
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_TN",
        "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_update_user_missing_state_code"
] = {
    "allowedSupervisionLocationIds": "",
    "allowedSupervisionLocationLevel": "",
    "blocked": False,
    "district": None,
    "emailAddress": "parameter@testdomain.com",
    "externalId": "UPDATED ID",
    "featureVariants": {},
    "firstName": "Updated",
    "lastName": "Name",
    "pseudonymizedId": "pseudo-UPDATED ID",
    "roles": ["leadership_role"],
    "routes": {},
    "stateCode": "US_TN",
    "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
}

snapshots["AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_update_users"] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "D1",
        "emailAddress": "parameter@testdomain.com",
        "externalId": "123",
        "featureVariants": {},
        "firstName": "Test",
        "lastName": "User",
        "pseudonymizedId": "pseudo-123",
        "roles": ["supervision_staff"],
        "routes": {},
        "stateCode": "US_CO",
        "userHash": "Sb6c3tejhmTMDZ3RmPVuSz2pLS7Eo2H4i/zaMrYfEMU=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "user@testdomain.com",
        "externalId": "456",
        "featureVariants": {},
        "firstName": "Original",
        "lastName": "Name",
        "pseudonymizedId": None,
        "roles": ["supervision_staff"],
        "routes": {},
        "stateCode": "US_TN",
        "userHash": "U9/nAUB/dvfqwBERoVETtCxT66GclnELpsw9OPrE9Vk=",
    },
]

snapshots["AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster"] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "ABC",
        "emailAddress": "facilities_staff@testdomain.com",
        "externalId": "2834",
        "featureVariants": {},
        "firstName": "facilities",
        "lastName": "user",
        "pseudonymizedId": "pseudo-2834",
        "roles": ["facilities_staff"],
        "routes": {"C": True},
        "stateCode": "US_XX",
        "userHash": "hAYT6YqEQZ2nuvlMgfr523mO4YE05n3wPcTCh9I6QBo=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "leadership@testdomain.com",
        "externalId": "3975",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3975",
        "roles": ["leadership_role"],
        "routes": {"A": True},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "supervision_staff@testdomain.com",
        "externalId": "3706",
        "featureVariants": {},
        "firstName": "supervision",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3706",
        "roles": ["supervision_staff"],
        "routes": {"B": True},
        "stateCode": "US_XX",
        "userHash": "_uYmjI0oMriD8yRXsTt1quVrTkZZuRHJ35X+szGMHJQ=",
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
        "emailAddress": "leadership@testdomain.com",
        "externalId": "1234",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {"A": True},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    }
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_multiple_roles"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "ABC",
        "emailAddress": "facilities_staff@testdomain.com",
        "externalId": "2834",
        "featureVariants": {},
        "firstName": "facilities",
        "lastName": "user",
        "pseudonymizedId": "pseudo-2834",
        "roles": ["facilities_staff", "supervision_staff", "experiment-foo"],
        "routes": {"B": True, "C": True},
        "stateCode": "US_XX",
        "userHash": "hAYT6YqEQZ2nuvlMgfr523mO4YE05n3wPcTCh9I6QBo=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "leadership@testdomain.com",
        "externalId": "3975",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3975",
        "roles": ["leadership_role", "supervision_staff"],
        "routes": {"A": True, "B": True},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "supervision_staff@testdomain.com",
        "externalId": "3706",
        "featureVariants": {},
        "firstName": "supervision",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3706",
        "roles": ["supervision_staff"],
        "routes": {"B": True},
        "stateCode": "US_XX",
        "userHash": "_uYmjI0oMriD8yRXsTt1quVrTkZZuRHJ35X+szGMHJQ=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_then_sync_roster"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "ABC",
        "emailAddress": "facilities_staff@testdomain.com",
        "externalId": "2834",
        "featureVariants": {},
        "firstName": "facilities",
        "lastName": "user",
        "pseudonymizedId": "pseudo-2834",
        "roles": ["facilities_staff"],
        "routes": {"C": True},
        "stateCode": "US_XX",
        "userHash": "hAYT6YqEQZ2nuvlMgfr523mO4YE05n3wPcTCh9I6QBo=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": None,
        "emailAddress": "leadership@testdomain.com",
        "externalId": "3975",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3975",
        "roles": ["leadership_role"],
        "routes": {"A": True},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "D1",
        "emailAddress": "supervision_staff@testdomain.com",
        "externalId": "3706",
        "featureVariants": {},
        "firstName": "supervision",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3706",
        "roles": ["supervision_staff"],
        "routes": {"B": True},
        "stateCode": "US_XX",
        "userHash": "_uYmjI0oMriD8yRXsTt1quVrTkZZuRHJ35X+szGMHJQ=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "D2",
        "emailAddress": "user@testdomain.com",
        "externalId": "98725",
        "featureVariants": {},
        "firstName": "supervision2",
        "lastName": "user2",
        "pseudonymizedId": "pseudo-98725",
        "roles": ["supervision_staff"],
        "routes": {"B": True},
        "stateCode": "US_XX",
        "userHash": "U9/nAUB/dvfqwBERoVETtCxT66GclnELpsw9OPrE9Vk=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_update_user"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@testdomain.com",
        "externalId": "3975",
        "featureVariants": {},
        "firstName": "leadership",
        "lastName": "user",
        "pseudonymizedId": "pseudo-3975",
        "roles": ["leadership_role", "supervision_staff"],
        "routes": {"A": True, "B": True},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    },
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "supervision_staff@testdomain.com",
        "externalId": None,
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["supervision_staff"],
        "routes": {"B": True},
        "stateCode": "US_XX",
        "userHash": "_uYmjI0oMriD8yRXsTt1quVrTkZZuRHJ35X+szGMHJQ=",
    },
]

snapshots[
    "AuthUsersEndpointTestCase.AuthUsersEndpointTestCase test_upload_roster_with_malformed_email_address"
] = [
    {
        "allowedSupervisionLocationIds": "",
        "allowedSupervisionLocationLevel": "",
        "blocked": False,
        "district": "",
        "emailAddress": "leadership@testdomain.com",
        "externalId": "0000",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
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
        "emailAddress": "leadership@testdomain.com",
        "externalId": "0000",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
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
        "emailAddress": "leadership@testdomain.com",
        "externalId": "0000",
        "featureVariants": {},
        "firstName": None,
        "lastName": None,
        "pseudonymizedId": None,
        "roles": ["leadership_role"],
        "routes": {},
        "stateCode": "US_XX",
        "userHash": "AeGKHtfy90TZ9wS9PoC8jtJKT9RdfMm1GLn1YPVqqBM=",
    }
]
