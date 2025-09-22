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
import { BigQueryNodeType, GraphType } from "../types";

export const rawGraphInfoFixture: GraphType = {
  nodes: [
    {
      urn: "fake-dataset.fake-table",
      viewId: "fake-table",
      datasetId: "fake-dataset",
      stateCode: null,
      type: BigQueryNodeType.VIEW,
    },
    {
      urn: "fake-dataset.fake-table-2",
      viewId: "fake-table-2",
      datasetId: "fake-dataset",
      stateCode: null,
      type: BigQueryNodeType.VIEW,
    },
    {
      urn: "fake-dataset.fake-table-3",
      viewId: "fake-table-3",
      datasetId: "fake-dataset",
      stateCode: null,
      type: BigQueryNodeType.VIEW,
    },
  ],
  references: [
    {
      source: "fake-dataset.fake-table",
      target: "fake-dataset.fake-table-2",
    },
    {
      source: "fake-dataset.fake-table",
      target: "fake-dataset.fake-table-3",
    },
  ],
};
