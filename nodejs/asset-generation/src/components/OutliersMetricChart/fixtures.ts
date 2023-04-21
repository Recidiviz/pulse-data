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

import { ChartData } from "./types";

export const officerData: ChartData = {
  min: 0.00039321014181349645,
  max: 0.19141444411147737,
  goal: 0.05,
  entities: [
    {
      name: "Eunice Ruiz",
      rate: 0.14282919193895519,
      goalStatus: "far",
      previousRate: 0.13782919193895518,
      previousGoalStatus: "far",
    },
    {
      name: "Wayne Mills",
      rate: 0.02347870834941464,
      goalStatus: "met",
      previousRate: 0.04547870834941464,
      previousGoalStatus: "met",
    },
    {
      name: "Alice Tate",
      rate: 0.1044349070100143,
      goalStatus: "near",
      previousRate: 0.13643490701001432,
      previousGoalStatus: "far",
    },
    {
      name: "Frederick Montgomery",
      rate: 0.1381006432084752,
      goalStatus: "far",
      previousRate: 0.1461006432084752,
      previousGoalStatus: "far",
    },
    {
      name: "Mamie White",
      rate: 0.03336380255941499,
      goalStatus: "met",
      previousRate: 0.06536380255941499,
      previousGoalStatus: "near",
    },
    {
      name: "Billy Marshall",
      rate: 0.1173935417470732,
      goalStatus: "near",
      previousRate: 0.1153935417470732,
      previousGoalStatus: "near",
    },
  ],
};

export const locationData: ChartData = {
  min: 0.0404321,
  max: 0.107144,
  goal: 0.05,
  entities: [
    {
      name: "Freeman",
      rate: 0.107144,
      goalStatus: "far",
      previousRate: 0.106144,
      previousGoalStatus: "far",
    },
    {
      name: "Gastonia",
      rate: 0.100144,
      goalStatus: "far",
      previousRate: 0.10087144,
      previousGoalStatus: "far",
    },
    {
      name: "Walshside",
      rate: 0.092144,
      goalStatus: "far",
      previousRate: 0.091144,
      previousGoalStatus: "far",
    },
    {
      name: "Berkshire",
      rate: 0.0862144,
      goalStatus: "far",
      previousRate: 0.08462144,
      previousGoalStatus: "far",
    },
    {
      name: "Mosciskiburgh",
      rate: 0.08462144,
      goalStatus: "far",
      previousRate: 0.0852144,
      previousGoalStatus: "far",
    },
    {
      name: "Avon",
      rate: 0.0832144,
      goalStatus: "far",
      previousRate: 0.08462144,
      previousGoalStatus: "far",
    },
    {
      name: "Dovieport",
      rate: 0.08314,
      goalStatus: "far",
      previousRate: 0.08384,
      previousGoalStatus: "far",
    },
    {
      name: "Sidney",
      rate: 0.07262144,
      goalStatus: "near",
      previousRate: 0.07131,
      previousGoalStatus: "near",
    },
    {
      name: "East Hartford",
      rate: 0.059726,
      goalStatus: "near",
      previousRate: 0.061726,
      previousGoalStatus: "near",
    },
    {
      name: "Jodyside",
      rate: 0.04139321,
      goalStatus: "met",
      previousRate: 0.0404321,
      previousGoalStatus: "met",
    },
  ],
};

export const districtData: ChartData = {
  min: 0.0404321,
  max: 0.107144,
  goal: 0.05,
  entities: [
    {
      name: "District 1",
      rate: 0.0832144,
      goalStatus: "far",
      previousRate: 0.08462144,
      previousGoalStatus: "far",
    },
    {
      name: "District 2",
      rate: 0.100144,
      goalStatus: "far",
      previousRate: 0.10087144,
      previousGoalStatus: "far",
    },

    {
      name: "District 3",
      rate: 0.0862144,
      goalStatus: "far",
      previousRate: 0.08462144,
      previousGoalStatus: "far",
    },
    {
      name: "District 4",
      rate: 0.092144,
      goalStatus: "far",
      previousRate: 0.091144,
      previousGoalStatus: "far",
    },
    {
      name: "District 5",
      rate: 0.059726,
      goalStatus: "near",
      previousRate: 0.061726,
      previousGoalStatus: "near",
    },
    {
      name: "District 6",
      rate: 0.04139321,
      goalStatus: "met",
      previousRate: 0.0404321,
      previousGoalStatus: "met",
    },
    {
      name: "District 7",
      rate: 0.08462144,
      goalStatus: "far",
      previousRate: 0.0852144,
      previousGoalStatus: "far",
    },
    {
      name: "District 8",
      rate: 0.107144,
      goalStatus: "far",
      previousRate: 0.106144,
      previousGoalStatus: "far",
    },

    {
      name: "District 9",
      rate: 0.08314,
      goalStatus: "far",
      previousRate: 0.08384,
      previousGoalStatus: "far",
    },
    {
      name: "District 10",
      rate: 0.07262144,
      goalStatus: "near",
      previousRate: 0.07131,
      previousGoalStatus: "near",
    },
  ],
};
