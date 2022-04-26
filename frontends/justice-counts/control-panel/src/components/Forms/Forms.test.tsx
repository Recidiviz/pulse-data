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

import { render, screen } from "@testing-library/react";
import React from "react";

import { TextInput } from ".";

test("Optional input label without value expected to be default position, font-size and color", () => {
  render(
    <TextInput
      type="text"
      label="Total Staff"
      valueLabel="People"
      context="Measures the number of full-time staff employed by the agency."
      value=""
      readOnly
    />
  );

  const label = screen.getByText(/Total Staff/i);

  expect(window.getComputedStyle(label).top).toBe("26px");
  expect(window.getComputedStyle(label).fontSize).toBe("1.25rem");
  expect(window.getComputedStyle(label).color).toBe("rgb(119, 119, 119)");

  expect.hasAssertions();
});

test("Required input label with value expected to shrink position, font-size and change color", () => {
  render(
    <TextInput
      type="text"
      label="Total Staff"
      valueLabel="People"
      context="Measures the number of full-time staff employed by the agency."
      value="100"
      readOnly
      required
    />
  );

  const label = screen.getByText(/Total Staff/i);

  expect(window.getComputedStyle(label).top).toBe("12px");
  expect(window.getComputedStyle(label).fontSize).toBe("0.75rem");
  expect(window.getComputedStyle(label).color).toBe("rgb(0, 115, 229)");

  expect.hasAssertions();
});

test("Context description appears in document", () => {
  render(
    <TextInput
      type="text"
      label="Total Staff"
      valueLabel="People"
      context="Measures the number of full-time staff employed by the agency."
      value="100"
      readOnly
      required
    />
  );

  const inputContext = screen.getByText(
    /Measures the number of full-time staff employed by the agency./i
  );

  expect(inputContext).toBeInTheDocument();

  expect.hasAssertions();
});
