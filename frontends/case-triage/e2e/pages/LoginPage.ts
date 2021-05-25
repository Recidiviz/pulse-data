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
import { waitForElement } from "../steps/helpers";

/* eslint-disable class-methods-use-this */
class LoginPage extends Page {
  open(): Promise<string> {
    // any page should trigger a redirect to login,
    // which is on the auth0 domain
    return this.visit("/");
  }

  get usernameInput() {
    return waitForElement(() => browser.queryByLabelText("Email"));
  }

  get passwordInput() {
    return waitForElement(() => browser.queryByLabelText("Password"));
  }

  get submitBtn() {
    return waitForElement(() => browser.queryByLabelText("Log In"));
  }

  async login(username: string, password: string): Promise<void> {
    const user = await this.usernameInput;
    await user.waitForDisplayed();
    await user.addValue(username);

    const pass = await this.passwordInput;
    await pass.waitForDisplayed();
    await pass.addValue(password);

    const btn = await this.submitBtn;
    await btn.waitForClickable();
    await btn.click();
  }
}

export default new LoginPage();
