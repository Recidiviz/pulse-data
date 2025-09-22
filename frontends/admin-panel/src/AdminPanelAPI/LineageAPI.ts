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

import {
  BigQuerySourceLineageDetail,
  BigQueryViewLineageDetail,
  GraphDirection,
  GraphType,
  NodeAncestorDependencies,
  NodeAncestorPath,
  NodeUrn,
} from "../LineageStore/types";
import { get } from "./utils";

export const fetchNodes = async (): Promise<GraphType> => {
  return get(`/admin/lineage/fetch_all`);
};

export const fetchViewMetadata = async (
  urn: NodeUrn
): Promise<BigQueryViewLineageDetail> => {
  return get(`/admin/lineage/metadata/view/${urn}`);
};

export const fetchSourceMetadata = async (
  urn: NodeUrn
): Promise<BigQuerySourceLineageDetail> => {
  return get(`/admin/lineage/metadata/source/${urn}`);
};

export const fetchAncestorUrns = async (
  direction: GraphDirection,
  urn: NodeUrn
): Promise<NodeAncestorDependencies> => {
  return get(`/admin/lineage/ancestors/${direction.toLowerCase()}/${urn}`);
};

export const fetchNodesBetween = async (
  direction: GraphDirection,
  startUrn: string,
  ancestorUrn: string
): Promise<NodeAncestorPath> => {
  return get(
    `/admin/lineage/between/${direction.toLowerCase()}/${startUrn}/${ancestorUrn}`
  );
};
