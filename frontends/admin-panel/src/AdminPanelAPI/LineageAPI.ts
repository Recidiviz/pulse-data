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

import { GraphType } from "../LineageStore/types";
import { get, getResource } from "./utils";

export const fetchNodes = async (): Promise<GraphType> => {
  return get(`/admin/lineage/fetch_all`);
};

export const fetchViewMetadata = async (
  datasetId: string,
  viewId: string
): Promise<GraphType> => {
  return get(`/admin/lineage/metadata/${datasetId}/${viewId}`);
};

export const fetchNodesBetween = async (
  startAddress: string,
  endAddress: string
): Promise<Response> => {
  return getResource(
    `/admin/lineage/fetch_between/${startAddress}/${endAddress}`
  );
};
