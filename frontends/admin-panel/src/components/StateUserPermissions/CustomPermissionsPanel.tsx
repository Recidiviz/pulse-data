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
import { FormInstance } from "antd";
import styled from "styled-components/macro";
import {
  PATHWAYS_PERMISSIONS_LABELS,
  VITALS_PERMISSIONS_LABELS,
  WORKFLOWS_PERMISSIONS_LABELS,
  OUTLIERS_PERMISSIONS_LABELS,
} from "../constants";
import FeatureVariantFormItem from "./FeatureVariantFormItem";
import PermissionSelect from "./PermissionSelect";
import { Route, StateUserPermissionsResponse } from "../../types";

const Note = styled.div`
  color: grey;
  padding-bottom: 5px;
`;

/**
 * Returns a value to use as the placeholder for the route dropdown, or undefined to show no
 * placeholder.
 *
 * If all selected users have the same value for the permission for the route (where "false" and
 * "undefined" are equivalent), use that value. Hide the placeholder if there are no selected users
 * or if the selected users have more than one distinct value for the permission for the route.
 */
export const routePlaceholder = (
  routeName: Route,
  selectedUsers?: StateUserPermissionsResponse[]
): string | undefined => {
  if (!selectedUsers) return undefined;

  const values = selectedUsers
    .map((u) => !!u.routes?.[routeName])
    .filter((v, i, a) => a.indexOf(v) === i);
  if (values.length > 1) return undefined;
  return values[0].toString();
};

export const CustomPermissionsPanel = ({
  hidePermissions,
  form,
  selectedUsers,
}: {
  hidePermissions: boolean;
  form: FormInstance;
  selectedUsers?: StateUserPermissionsResponse[];
}): JSX.Element => (
  <>
    <h3>Custom Permissions</h3>

    <h4>Workflows:</h4>
    {(
      Object.entries(WORKFLOWS_PERMISSIONS_LABELS) as [
        keyof typeof WORKFLOWS_PERMISSIONS_LABELS,
        string
      ][]
    ).map(([name, label]) => {
      return (
        <PermissionSelect
          permission={{ name, label }}
          key={name}
          disabled={hidePermissions}
          placeholder={
            hidePermissions ? undefined : routePlaceholder(name, selectedUsers)
          }
        />
      );
    })}

    <h4>Outliers:</h4>
    {(
      Object.entries(OUTLIERS_PERMISSIONS_LABELS) as [
        keyof typeof OUTLIERS_PERMISSIONS_LABELS,
        string
      ][]
    ).map(([name, label]) => {
      return (
        <PermissionSelect
          permission={{ name, label }}
          key={name}
          disabled={hidePermissions}
          placeholder={
            hidePermissions ? undefined : routePlaceholder(name, selectedUsers)
          }
        />
      );
    })}

    <h4>Vitals (Operations):</h4>
    {(
      Object.entries(VITALS_PERMISSIONS_LABELS) as [
        keyof typeof VITALS_PERMISSIONS_LABELS,
        string
      ][]
    ).map(([name, label]) => {
      return (
        <PermissionSelect
          permission={{ name, label }}
          key={name}
          disabled={hidePermissions}
          placeholder={
            hidePermissions ? undefined : routePlaceholder(name, selectedUsers)
          }
        />
      );
    })}

    <h4>Pathways Pages:</h4>
    {(
      Object.entries(PATHWAYS_PERMISSIONS_LABELS) as [
        keyof typeof PATHWAYS_PERMISSIONS_LABELS,
        string
      ][]
    ).map(([name, label]) => {
      return (
        <PermissionSelect
          permission={{ name, label }}
          key={name}
          disabled={hidePermissions}
          placeholder={
            hidePermissions ? undefined : routePlaceholder(name, selectedUsers)
          }
        />
      );
    })}

    <h4>Feature Variants:</h4>
    <Note>Notes:</Note>
    <Note>- Enter feature variant id to enable/disable it.</Note>
    <Note>
      - Once the variant is enabled by selecting &quot;True&quot;, an
      &quot;Active Date&quot; can also be specified if the feature should become
      active at a specific date and time (in *your* time zone), otherwise the
      variant will be active immediately
    </Note>
    <Note>
      - Disabling the variant by selecting &quot;False&quot; always takes place
      immediately.
    </Note>
    <FeatureVariantFormItem disabled={hidePermissions} form={form} />
  </>
);

export default CustomPermissionsPanel;
