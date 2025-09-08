// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2025 Recidiviz, Inc.
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
import { GraphType } from "../types";

export const rawGraphInfoFixture: GraphType = {
  nodes: [
    {
      urn: "fake-dataset.fake-table",
    },
    {
      urn: "fake-dataset.fake-table-2",
    },
    {
      urn: "fake-dataset.fake-table-3",
    },
  ],
  references: [
    {
      sourceUrn: "fake-dataset.fake-table",
      targetUrn: "fake-dataset.fake-table-2",
    },
    {
      sourceUrn: "fake-dataset.fake-table",
      targetUrn: "fake-dataset.fake-table-3",
    },
  ],
};
