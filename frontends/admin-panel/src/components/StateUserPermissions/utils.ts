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
import { useRef, useState } from "react";
import type { DraggableData, DraggableEvent } from "react-draggable";
import { Routes } from "./types";

interface DraggableConstants {
  disabled: boolean;
  setDisabled: (arg0: boolean) => void;
  bounds: Record<string, number>;
  dragRef: React.LegacyRef<HTMLDivElement> | undefined;
  onStart: (_event: DraggableEvent, uiData: DraggableData) => void;
}

export function DraggableModal(): DraggableConstants {
  const [disabled, setDisabled] = useState(false);
  const [bounds, setBounds] = useState({
    left: 0,
    top: 0,
    bottom: 0,
    right: 0,
  });
  const dragRef = useRef<HTMLInputElement>(null);
  const onStart = (_event: DraggableEvent, uiData: DraggableData) => {
    const { clientWidth, clientHeight } = window.document.documentElement;
    const targetRect = dragRef.current?.getBoundingClientRect();
    if (!targetRect) {
      return;
    }
    setBounds({
      left: -targetRect.left + uiData.x,
      right: clientWidth - (targetRect.right - uiData.x),
      top: -targetRect.top + uiData.y,
      bottom: clientHeight - (targetRect.bottom - uiData.y),
    });
  };
  return { disabled, setDisabled, bounds, dragRef, onStart };
}

export const updatePermissionsObject = (
  existingRoutes: Routes,
  updatedRoutes: Partial<StateUserPermissionsResponse>,
  validPermissions: Record<string, string>
): Routes | undefined => {
  const newRoutes = Object.entries(updatedRoutes).reduce(
    (permissions, [permissionType, permissionValue]) => {
      if (
        Object.keys(validPermissions).includes(permissionType) &&
        permissionValue !== undefined
      ) {
        return {
          ...permissions,
          // If the permission type is a route or feature variant (requirement of this logic branch),
          // the value will be a boolean so typecasting is safe
          [permissionType]: permissionValue as boolean,
        };
      }
      return { ...permissions };
    },
    existingRoutes
  );
  return Object.keys(newRoutes).length > 0 ? newRoutes : undefined;
};

export const checkResponse = async (response: Response): Promise<void> => {
  if (!response.ok) {
    const error = await response.text();
    throw error;
  }
};
