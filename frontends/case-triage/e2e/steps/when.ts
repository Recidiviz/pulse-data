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
import homePage, { LIST_TYPE } from "../pages/HomePage";
import loginPage from "../pages/LoginPage";
import { isUserLevel } from "./types";
import { NEED_TYPE } from "./helpers";

When("I visit the home page", async () => {
  await homePage.open();
});

When("I login as {string}", async (userLevel: string) => {
  if (!isUserLevel(userLevel)) throw new Error("invalid user level specified");

  const { username, password } = browser.config.credentials[userLevel];

  await loginPage.login(username, password);
});

When("I debug", { timeout: 999999999 }, async () => {
  await browser.debug();
});

When("I take a screenshot {string}", async (path: string) => {
  await browser.saveScreenshot(path);
});

When("I click the {string} dropdown button", async (text: string) => {
  const dropdownButtonElement = await homePage.findButton(text);
  await expect(dropdownButtonElement).toBeDisplayed();
  await dropdownButtonElement.click();
});

When(
  "I fill in the {string} field with {string}",
  async (label: string, text: string) => {
    const input = await homePage.findTextInput(label);
    await input.addValue(text);
  }
);

When("I click the {string} button", async (text: string) => {
  const button = await homePage.findButton(text);
  await expect(button).toBeClickable();
  await button.click();
});

When(
  "I click to submit a(n) {string} correction to {string}'s case",
  async (correction: string, name: string) => {
    const caseCard = await homePage.findCaseCard(name);
    await expect(caseCard).toBeDisplayed();
    const dropdownToggle = await homePage.findCorrectionToggleButton(
      caseCard,
      0
    );
    await dropdownToggle.click();
    const correctionOption = await homePage.findCorrectionButton(
      caseCard,
      correction
    );
    await expect(correctionOption).toBeDisplayed();
    await correctionOption.click();
  }
);

When(
  "I undo the active action inside the {string} need",
  async (need: NEED_TYPE) => {
    const needElement = await homePage.findNeedElement(need);
    await expect(needElement).toBeDisplayed();
    const removeButton = await homePage.findUndoActionButton(needElement);
    await removeButton.waitForClickable();
    await expect(removeButton).toBeDisplayed();
    await removeButton.click();
  }
);

When(
  "I click to submit a(n) {string} correction to the {string} need",
  async (correction: string, need: NEED_TYPE) => {
    const needElement = await homePage.findNeedElement(need);
    await expect(needElement).toBeDisplayed();
    const dropdownToggle = await homePage.findCorrectionToggleButton(
      needElement
    );
    await dropdownToggle.click();
    const correctionOption = await homePage.findCorrectionButton(
      needElement,
      correction
    );
    await expect(correctionOption).toBeDisplayed();
    await correctionOption.click();
  }
);

When(
  "I open {string}'s case card from the {string} list",
  async (name: string, list: LIST_TYPE) => {
    const clientListCard = await homePage.findClientListCardByName(list, name);
    await expect(clientListCard).toBeDisplayed();
    await clientListCard.click();
    const caseCard = await homePage.findCaseCard(name);
    await expect(caseCard).toBeDisplayed();
  }
);

When("I complete the {string} need action", async (text: string) => {
  const needAction = await homePage.findButton(text);
  await expect(needAction).toBeDisplayed();
  await needAction.click();
});
