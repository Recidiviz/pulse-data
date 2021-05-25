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
import { WaitUntilOptions } from "webdriverio";
import TEST_IDS from "../../src/components/TestIDs";

export const waitForElement = async (
  getElement: () => Promise<WebdriverIO.Element>,
  matches?: (element: WebdriverIO.Element) => Promise<boolean>,
  options?: WaitUntilOptions
): Promise<WebdriverIO.Element> => {
  let element;

  await browser.waitUntil(async () => {
    element = await getElement();

    if (matches) {
      return matches(element);
    }

    return !!element;
  }, options);

  if (!element) {
    throw new Error("Could not find element");
  }

  return element;
};

export const waitUntilElementHasText = async (
  getElement: () => Promise<WebdriverIO.Element>,
  text: string
): Promise<WebdriverIO.Element> => {
  // If the element was selected just prior to React re-rendering, it may not have the expected text.
  // Wait until it is has the specified text.
  return waitForElement(
    getElement,
    async (element) => (await element.getText()).indexOf(text) !== -1,
    { timeoutMsg: `Expected element to have text ${text}` }
  );
};

export const waitUntilElementHasChildren = async (
  getElement: () => Promise<WebdriverIO.Element>
): Promise<WebdriverIO.Element> => {
  return waitForElement(
    getElement,
    async (element) => (await element.$$("*")).length > 0,
    { timeoutMsg: "Expected element to have child element(s)" }
  );
};

export type NEED_TYPE =
  | "employment"
  | "risk assessment"
  | "face to face contact";

export const NEED_TEST_ID_MAP: Record<NEED_TYPE, string> = {
  employment: TEST_IDS.NEEDS_EMPLOYMENT,
  "risk assessment": TEST_IDS.NEEDS_RISK_ASSESSMENT,
  "face to face contact": TEST_IDS.NEEDS_FACE_TO_FACE_CONTACT,
};
