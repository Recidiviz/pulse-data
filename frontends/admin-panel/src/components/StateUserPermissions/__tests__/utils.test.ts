// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2025 Recidiviz, Inc.
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

import { addWeeks, subWeeks } from "date-fns";

import { FeatureVariants, RouteRecord } from "../../../types";
import {
  checkPermissionsEquivalence,
  mergeFeatureVariants,
  mergeRoutes,
} from "../utils";

const activeDate1 = new Date("2024-04-30T14:45:09.865Z");
const activeDate2 = new Date("2024-05-03T15:37:07.119Z");

describe("mergeRoutes", () => {
  test("merges routes for single role", () => {
    const routesSingleRoleTestData: RouteRecord[] = [
      {
        lantern: true,
        psi: false,
        system_libertyToPrison: false,
        system_prison: false,
      },
    ];

    expect(mergeRoutes(routesSingleRoleTestData)).toEqual({
      lantern: true,
      psi: false,
      system_libertyToPrison: false,
      system_prison: false,
    });
  });

  test("merges routes for multiple roles without conflicts", () => {
    const routesMultipleRolesWithoutConflictsTestData: RouteRecord[] = [
      {
        lantern: true,
        psi: false,
      },
      {
        psi: false,
        system_prison: true,
      },
    ];

    expect(mergeRoutes(routesMultipleRolesWithoutConflictsTestData)).toEqual({
      lantern: true,
      psi: false,
      system_prison: true,
    });
  });

  test("merges routes for multiple roles with conflicts", () => {
    const routesMultipleRolesWithConflictsTestData: RouteRecord[] = [
      {
        lantern: true,
        psi: false,
      },
      {
        lantern: false,
        psi: true,
        system_prison: true,
      },
    ];

    expect(mergeRoutes(routesMultipleRolesWithConflictsTestData)).toEqual({
      lantern: true,
      psi: true,
      system_prison: true,
    });
  });
});

describe("mergeFeatureVariants", () => {
  test("merges feature variants for single role", () => {
    const featureVariantsSingleRoleTestData: FeatureVariants[] = [
      { feature1: {}, feature2: { activeDate: activeDate1 } },
    ];

    expect(mergeFeatureVariants(featureVariantsSingleRoleTestData)).toEqual({
      feature1: {},
      feature2: { activeDate: activeDate1 },
    });
  });

  test("merges feature variants for multiple roles without conflicts", () => {
    const featureVariantsMultipleRolesNoConflictTestData: FeatureVariants[] = [
      { feature1: false },
      { feature2: {} },
    ];

    expect(
      mergeFeatureVariants(featureVariantsMultipleRolesNoConflictTestData)
    ).toEqual({ feature1: false, feature2: {} });
  });

  test("merges feature variants for multiple roles with conflicts", () => {
    const featureVariantsMultipleRolesWithConflictsTestData: FeatureVariants[] =
      [
        {
          feature1: false,
          feature2: { activeDate: activeDate1 },
          feature3: false,
          feature4: { activeDate: activeDate2 },
          feature5: {},
          feature6: { activeDate: activeDate1 },
        },
        {
          feature1: {},
          feature2: {},
          feature3: { activeDate: activeDate2 },
          feature4: { activeDate: activeDate1 },
          feature5: { activeDate: activeDate2 },
          feature6: false,
        },
      ];

    expect(
      mergeFeatureVariants(featureVariantsMultipleRolesWithConflictsTestData)
    ).toEqual({
      feature1: {},
      feature2: {},
      feature3: { activeDate: activeDate2 },
      feature4: { activeDate: activeDate1 },
      feature5: {},
      feature6: { activeDate: activeDate1 },
    });
  });
});

describe("checkPermissionsEquivalence", () => {
  test("equivalent when default permissions are always on", () => {
    expect(checkPermissionsEquivalence({}, {})).toBe(true);
  });

  test("equivalent when default permissions have a past activeDate", () => {
    const defaultPermissionsPastDate = new Date("2025-01-01");
    const userPermissionsPastDate = subWeeks(defaultPermissionsPastDate, 1);
    expect(
      checkPermissionsEquivalence(
        { activeDate: defaultPermissionsPastDate },
        {}
      )
    ).toBe(true);
    expect(
      checkPermissionsEquivalence(
        { activeDate: defaultPermissionsPastDate },
        { activeDate: userPermissionsPastDate }
      )
    ).toBe(true);
  });

  test("equivalent when both default and user permissions have a future activeDate", () => {
    const defaultPermissionsFutureDate = addWeeks(new Date(), 1);
    const userPermissionsFutureDate = addWeeks(new Date(), 2);
    expect(
      checkPermissionsEquivalence(
        { activeDate: defaultPermissionsFutureDate },
        { activeDate: userPermissionsFutureDate }
      )
    ).toBe(true);
  });

  test("not equivalent when default permissions have a future date and user permissions are always on", () => {
    const defaultPermissionsFutureDate = addWeeks(new Date(), 1);
    expect(
      checkPermissionsEquivalence(
        { activeDate: defaultPermissionsFutureDate },
        {}
      )
    ).toBe(false);
  });

  test("not equivalent when default permissions have a future date and user permissions have a past date", () => {
    const defaultPermissionsFutureDate = addWeeks(new Date(), 1);
    const userPermissionsPastDate = subWeeks(new Date(), 1);
    expect(
      checkPermissionsEquivalence(
        { activeDate: defaultPermissionsFutureDate },
        { activeDate: userPermissionsPastDate }
      )
    ).toBe(false);
  });

  test("equivalent when default permissions are true", () => {
    expect(checkPermissionsEquivalence(true, true)).toBe(true);
  });

  test("equivalent when both default and user permissions are false", () => {
    expect(checkPermissionsEquivalence(false, false)).toBe(true);
  });

  test("not equivalent when default permissions are false and user permissions are true", () => {
    expect(checkPermissionsEquivalence(false, true)).toBe(false);
  });
});
