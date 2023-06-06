// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

import type { Meta, StoryObj } from "@storybook/react";

import {
  fittingUnitDataTransformed,
  overflowingUnitDataTransformed,
} from "./fixtures";
import { OutliersUnitChart } from "./OutliersUnitChart";

const meta: Meta<typeof OutliersUnitChart> = {
  title: "OutliersUnitChart",
  component: OutliersUnitChart,
  argTypes: {
    syncHeight: { action: "syncHeight" },
  },
};

export default meta;
type Story = StoryObj<typeof OutliersUnitChart>;

const render: Story["render"] = (props) => <OutliersUnitChart {...props} />;

const width = 570;

export const WithFittingUnit: Story = {
  render,
  args: {
    width,
    data: fittingUnitDataTransformed,
  },
};

export const WithOverflowingUnit: Story = {
  render,
  args: {
    width,
    data: overflowingUnitDataTransformed,
  },
};
