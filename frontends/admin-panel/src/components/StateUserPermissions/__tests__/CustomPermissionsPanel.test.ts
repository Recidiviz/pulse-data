// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.
// =============================================================================

import { StateUserPermissionsResponse } from "../../../types";
import { routePlaceholder } from "../CustomPermissionsPanel";

const baseUser: Omit<StateUserPermissionsResponse, "routes"> = {
  allowedSupervisionLocationIds: "test",
  allowedSupervisionLocationLevel: "test",
  blocked: false,
  district: "test",
  emailAddress: "test",
  externalId: "test",
  featureVariants: {},
  firstName: "test",
  lastName: "test",
  role: "test",
  stateCode: "test",
  userHash: "test",
};

describe("routePlaceholder", () => {
  test("no selected users", () => {
    expect(routePlaceholder("workflows")).toBeUndefined();
  });

  test.each([
    [true, "true"],
    [false, "false"],
    [undefined, "false"],
  ])("single user with permissions for route", (permission, expected) => {
    const selectedUsers = [
      {
        ...baseUser,
        routes: {
          workflows: permission,
        },
      },
    ];
    expect(routePlaceholder("workflows", selectedUsers)).toEqual(expected);
  });

  test.each([
    [true, "true"],
    [false, "false"],
    [undefined, "false"],
  ])("users with same permissions for route", (permission, expected) => {
    const selectedUsers = [
      {
        ...baseUser,
        routes: {
          workflows: permission,
        },
      },
      {
        ...baseUser,
        routes: {
          workflows: permission,
        },
      },
    ];
    expect(routePlaceholder("workflows", selectedUsers)).toEqual(expected);
  });

  test("users with different permissions", () => {
    const selectedUsers = [
      {
        ...baseUser,
        routes: {
          workflows: true,
        },
      },
      {
        ...baseUser,
        routes: {},
      },
    ];
    expect(routePlaceholder("workflows", selectedUsers)).toBeUndefined();
  });

  test("user with null routes", () => {
    const selectedUsers = [
      {
        ...baseUser,
        routes: null,
      },
    ];
    expect(routePlaceholder("workflows", selectedUsers)).toEqual("false");
  });
});
