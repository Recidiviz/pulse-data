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

import Page from "./Page";
import TEST_IDS from "../../src/components/TestIDs";
import {
  NEED_TEST_ID_MAP,
  NEED_TYPE,
  waitForElement,
  waitUntilElementHasText,
} from "../steps/helpers";

export type LIST_TYPE = "Processing Feedback" | "Up Next";

/* eslint-disable class-methods-use-this */
class HomePage extends Page {
  open(): Promise<string> {
    return this.visit("/");
  }

  get clientListHeading() {
    return waitForElement(() =>
      browser.queryByRole("heading", { level: 2, name: "Up Next" })
    );
  }

  async findClientList(list: LIST_TYPE) {
    const listTestID =
      list === "Processing Feedback"
        ? TEST_IDS.PROCESSING_FEEDBACK_LIST
        : TEST_IDS.UP_NEXT_LIST;

    return waitForElement(() => browser.getByTestId(listTestID));
  }

  async findClientListCardByName(list: LIST_TYPE, name: string) {
    const listElement = await this.findClientList(list);

    return listElement.getByText(name);
  }

  async findClientListCardByPosition(
    position: "top" | "bottom" | string,
    list: LIST_TYPE
  ) {
    const listElement = await this.findClientList(list);

    const listItems = await listElement.$$(`.client-card`);

    if (position === "top") {
      return listItems[0];
    }
    if (position === "bottom") {
      return listItems[listItems.length - 1];
    }

    const index = position.substr(0, position.length - "th spot".length);
    return listItems[parseInt(index) - 1];
  }

  findNeedElement(need: NEED_TYPE): Promise<WebdriverIO.Element> {
    return browser.getByTestId(NEED_TEST_ID_MAP[need]);
  }

  async findUndoActionButton(container: WebdriverIO.Element) {
    return container.getByLabelText("Remove");
  }

  async findCorrectionToggleButton(
    container: WebdriverIO.Element,
    index?: number
  ) {
    if (index !== undefined) {
      return (await container.findAllByLabelText("Submit a correction"))[index];
    }
    return container.getByLabelText("Submit a correction");
  }

  findCaseCard(name: string): Promise<WebdriverIO.Element> {
    return waitUntilElementHasText(
      () => browser.getByTestId(TEST_IDS.CASE_CARD),
      name
    );
  }

  async findButton(text: string): Promise<WebdriverIO.Element> {
    return browser.getByText(text, { selector: "button" });
  }

  async findModal(): Promise<WebdriverIO.Element> {
    return browser.getByRole("dialog");
  }

  async findTextInput(label: string): Promise<WebdriverIO.Element> {
    return browser.getByLabelText(label);
  }

  async findCorrectionButton(
    container: WebdriverIO.Element,
    correction: string
  ) {
    return container.getByText(correction);
  }
}

export default new HomePage();
