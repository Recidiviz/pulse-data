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

import { OutliersAggregatedChartInput } from "../../server/generate/outliersAggregatedChart/types";

export const supervisorsData: OutliersAggregatedChartInput["data"] = {
  target: 0.07095,
  entities: [
    {
      name: "Jon Bell",
      officersFarPct: 0.5,
      prevOfficersFarPct: 0.5,
      officerRates: {
        FAR: [0.4384812],
        NEAR: [0.0936552],
      },
    },
    {
      name: "Todd Harris",
      officersFarPct: 0.4,
      prevOfficersFarPct: 0.2,
      officerRates: {
        MET: [0.065686447, 0, 0.01844],
        FAR: [0.146317, 0.180424],
      },
    },
    {
      name: "Gordon Quinn",
      officersFarPct: 0.25,
      prevOfficersFarPct: 0.25,
      officerRates: {
        MET: [0.04852, 0.0202, 0.03025],
        FAR: [0.153547],
      },
    },
    {
      name: "Jesus Hughes",
      officersFarPct: 0,
      prevOfficersFarPct: 0,
      officerRates: {
        MET: [0.0334, 0.0619, 0, 0.0292, 0.0572],
        NEAR: [0.091, 0.1759],
      },
    },
    {
      name: "Nathan Singleton",
      officersFarPct: 0,
      prevOfficersFarPct: 0,
      officerRates: {
        NEAR: [0.07664, 0.087, 0.1037],
        MET: [0.0359, 0, 0.0446],
      },
    },
    {
      name: "Harriett Caldwell",
      officersFarPct: 0,
      prevOfficersFarPct: 0,
      officerRates: {
        NEAR: [0.0787],
        MET: [0.03082, 0.0685, 0.0304, 0],
      },
    },
  ],
};
