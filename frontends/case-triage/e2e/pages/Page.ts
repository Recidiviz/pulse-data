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

/* eslint-disable class-methods-use-this */
import { setupBrowser } from "@testing-library/webdriverio";

setupBrowser(browser);

export default class Page {
  async visit(path: string): Promise<string> {
    return browser.url(path);
  }

  async setupSuite(): Promise<string> {
    return this.visit(`http://localhost:5000/e2e/setup_suite`);
  }

  async teardownScenario(): Promise<string> {
    return this.visit(`http://localhost:5000/e2e/teardown_scenario`);
  }
}
