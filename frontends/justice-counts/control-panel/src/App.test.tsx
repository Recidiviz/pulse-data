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
import { render, screen } from "@testing-library/react";
import React from "react";

import App from "./App";
import { StoreProvider } from "./stores/StoreProvider";

jest.mock("@auth0/auth0-spa-js");

const mockCreateAuth0Client = createAuth0Client as jest.Mock;
const mockIsAuthenticated = jest.fn();
const mockLoginWithRedirect = jest.fn();

beforeEach(() => {
  mockCreateAuth0Client.mockResolvedValue({
    isAuthenticated: mockIsAuthenticated,
    loginWithRedirect: mockLoginWithRedirect,
  });
});

afterEach(() => {
  jest.resetAllMocks();
});

test("renders loading on load", () => {
  render(
    <StoreProvider>
      <App />
    </StoreProvider>
  );
  const loadingText = screen.getByText(/Loading data.../i);
  expect(loadingText).toBeInTheDocument();
});
