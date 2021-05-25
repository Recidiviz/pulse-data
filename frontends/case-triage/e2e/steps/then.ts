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

import { Then } from "@cucumber/cucumber";
import homePage from "../pages/HomePage";
import {
  NEED_TEST_ID_MAP,
  NEED_TYPE,
  waitUntilElementHasText,
} from "./helpers";

Then("I should see my client list", async () => {
  const heading = await homePage.clientListHeading;
  await expect(heading).toBeDisplayed();
});

Then(
  "the {string} need should have an {string} action {string}",
  async (need: NEED_TYPE, active: "active" | "inactive", text: string) => {
    await waitUntilElementHasText(() => homePage.findNeedElement(need), text);
  }
);

Then("I should see a modal with text {string}", async (text: string) => {
  await waitUntilElementHasText(() => homePage.findModal(), text);
});

Then("I should not see a modal", async () => {
  await browser.waitUntil(async () => {
    try {
      return !(await homePage.findModal()).isVisible();
    } catch (error) {
      return true;
    }
  });
});

Then(
  "the {string} need should say {string}",
  async (need: NEED_TYPE, text: string) => {
    await waitUntilElementHasText(() => homePage.findNeedElement(need), text);
  }
);

Then(
  "I should see {string} at the {string} of the {string} list",
  async (
    client: string,
    position: "top" | "bottom" | string,
    list: "Processing Feedback" | "Up Next"
  ) => {
    await waitUntilElementHasText(
      () => homePage.findClientListCardByPosition(position, list),
      client
    );
  }
);
