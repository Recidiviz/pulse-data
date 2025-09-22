// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { createContext, useContext } from "react";

import { GraphStore } from "./GraphStore";
import { LineageRootStore } from "./LineageRootStore";
import { LineageStore } from "./LineageStore";
import { UiStore } from "./UiStore";

export const LineageRootContext = createContext<LineageRootStore | undefined>(
  undefined
);

export function useLineageRootStore(): LineageRootStore {
  const context = useContext(LineageRootContext);
  if (context === undefined) {
    throw new Error("Must call GraphContext within a valid provider");
  }
  return context;
}

export function useLineageStore(): LineageStore {
  return useLineageRootStore().lineageStore;
}

export function useGraphStore(): GraphStore {
  return useLineageRootStore().graphStore;
}

export function useUiStore(): UiStore {
  return useLineageRootStore().uiStore;
}
