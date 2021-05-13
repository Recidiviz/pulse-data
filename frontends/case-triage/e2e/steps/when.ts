// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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

import { When } from "@cucumber/cucumber";
import homePage from "../pages/HomePage";
import loginPage from "../pages/LoginPage";
import { isUserLevel } from "./types";

When("I visit the home page", () => {
  homePage.open();
});

When("I login as {string}", async (userLevel: string) => {
  if (!isUserLevel(userLevel)) throw new Error("invalid user level specified");

  const { username, password } = browser.config.credentials[userLevel];

  await loginPage.login(username, password);
});
