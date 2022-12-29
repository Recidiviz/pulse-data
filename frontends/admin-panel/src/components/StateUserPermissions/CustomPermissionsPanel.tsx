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

import PermissionSelect from "./PermissionSelect";
import {
  GENERAL_PERMISSIONS_LABELS,
  PATHWAYS_PERMISSIONS_LABELS,
  VITALS_PERMISSIONS_LABELS,
  WORKFLOWS_PERMISSIONS_LABELS,
} from "./types";

export const CustomPermissionsPanel = ({
  hidePermissions,
}: {
  hidePermissions: boolean;
}): JSX.Element => {
  return (
    <>
      <h3>Custom Permissions</h3>
      <h4>General</h4>
      <hr />
      {Object.entries(GENERAL_PERMISSIONS_LABELS).map(([name, label]) => {
        return (
          <PermissionSelect
            permission={{ name, label }}
            disabled={hidePermissions}
          />
        );
      })}

      <h4>Workflows</h4>
      <hr />
      {Object.entries(WORKFLOWS_PERMISSIONS_LABELS).map(([name, label]) => {
        return (
          <PermissionSelect
            permission={{ name, label }}
            disabled={hidePermissions}
          />
        );
      })}

      <h4>Vitals (Operations)</h4>
      <hr />
      {Object.entries(VITALS_PERMISSIONS_LABELS).map(([name, label]) => {
        return (
          <PermissionSelect
            permission={{ name, label }}
            disabled={hidePermissions}
          />
        );
      })}

      <h4>Pathways Pages</h4>
      <hr />
      {Object.entries(PATHWAYS_PERMISSIONS_LABELS).map(([name, label]) => {
        return (
          <PermissionSelect
            permission={{ name, label }}
            disabled={hidePermissions}
          />
        );
      })}
    </>
  );
};

export default CustomPermissionsPanel;
