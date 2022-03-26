// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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

import createAuth0Client from "@auth0/auth0-spa-js";

import { rootStore } from "../../stores";
import { AuthStore } from "./AuthStore";

jest.mock("@auth0/auth0-spa-js");

const mockCreateAuth0Client = createAuth0Client as jest.Mock;
const mockGetUser = jest.fn();
const mockIsAuthenticated = jest.fn();
const mockLoginWithRedirect = jest.fn();

const testAuthSettings = {
  domain: "test.auth0.com",
  client_id: "testclientid",
  redirect_url: window.location.href,
};

beforeEach(() => {
  mockCreateAuth0Client.mockResolvedValue({
    getUser: mockGetUser,
    isAuthenticated: mockIsAuthenticated,
    loginWithRedirect: mockLoginWithRedirect,
  });
});

afterEach(() => {
  jest.resetAllMocks();
});

const store = new AuthStore({ authSettings: testAuthSettings, rootStore });

test("authorization pending when required", async () => {
  expect(store.isAuthorized).toBe(false);
  expect(store.isLoading).toBe(true);

  expect.hasAssertions();
});

test("authorized when authenticated", async () => {
  mockIsAuthenticated.mockResolvedValue(true);

  await store.authenticate();
  expect(store.isAuthorized).toBe(true);
  expect(store.isLoading).toBe(false);

  expect.hasAssertions();
});

test("requires email verification", async () => {
  mockGetUser.mockResolvedValue({ emailVerified: false });
  mockIsAuthenticated.mockResolvedValue(true);

  await store.authenticate();
  expect(store.isAuthorized).toBe(true);
  expect(store.emailVerified).toBe(false);

  expect.hasAssertions();
});

test("redirect to Auth0 when unauthenticated", async () => {
  mockIsAuthenticated.mockResolvedValue(false);
  expect(mockLoginWithRedirect.mock.calls.length).toBe(0);

  await store.authenticate();
  expect(mockLoginWithRedirect.mock.calls.length).toBe(1);
  expect(mockLoginWithRedirect.mock.calls[0][0]).toEqual({
    appState: { targetUrl: window.location.href },
  });

  expect.hasAssertions();
});
