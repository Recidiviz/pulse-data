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

import { AllowedApp, Route, StateUserPermissionsResponse } from "../../types";
import {
  ALLOWED_APPS_LABELS,
  CPA_PERMISSIONS_LABELS,
  INSIGHTS_PERMISSIONS_LABELS,
  LANTERN_PERMISSIONS_LABELS,
  MEETINGS_PERMISSIONS_LABELS,
  PATHWAYS_PERMISSIONS_LABELS,
  PSI_PERMISSIONS_LABELS,
  ROUTES_PERMISSIONS_LABELS,
  VITALS_PERMISSIONS_LABELS,
  WORKFLOWS_PERMISSIONS_LABELS,
} from "../constants";
import FeatureVariantFormItem from "./FeatureVariantFormItem";
import PermissionSelect from "./PermissionSelect";
import { Note } from "./styles";

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

function BasicPermissionList<PermissionType extends Route | AllowedApp>({
  labels,
  hidePermissions,
  selectedUsers,
  getPlaceholder = () => undefined,
}: {
  labels: Partial<Record<PermissionType, string>>;
  hidePermissions: boolean;
  selectedUsers?: StateUserPermissionsResponse[];
  getPlaceholder?: (
    routeName: PermissionType,
    selectedUsers: StateUserPermissionsResponse[] | undefined
  ) => string | undefined;
}): JSX.Element {
  return (
    <>
      {(Object.entries(labels) as [keyof typeof labels, string][]).map(
        ([name, label]) => {
          return (
            <PermissionSelect
              permission={{ name, label }}
              key={name}
              disabled={hidePermissions}
              placeholder={
                hidePermissions
                  ? undefined
                  : getPlaceholder(name, selectedUsers)
              }
            />
          );
        }
      )}
    </>
  );
}

const RoutePermissionList = (props: {
  labels: Partial<typeof ROUTES_PERMISSIONS_LABELS>;
  hidePermissions: boolean;
  selectedUsers?: StateUserPermissionsResponse[];
}): JSX.Element => (
  <BasicPermissionList getPlaceholder={routePlaceholder} {...props} />
);

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
    <h4>App access:</h4>
    <BasicPermissionList
      labels={ALLOWED_APPS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />

    <h4>Workflows:</h4>
    <RoutePermissionList
      labels={WORKFLOWS_PERMISSIONS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />

    <h4>Supervisor Homepage (Insights):</h4>
    <PermissionSelect
      permission={{
        name: "insights",
        label: INSIGHTS_PERMISSIONS_LABELS.insights,
      }}
      disabled={hidePermissions}
      placeholder={
        hidePermissions
          ? undefined
          : routePlaceholder("insights", selectedUsers)
      }
      dependencies={["insights_supervision_supervisors-list"]}
      rules={[
        ({ getFieldValue }) => ({
          // require an explicit value to ensure no conflict with this field
          required: getFieldValue("insights_supervision_supervisors-list"),
          message: `${INSIGHTS_PERMISSIONS_LABELS.insights} is required when ${INSIGHTS_PERMISSIONS_LABELS["insights_supervision_supervisors-list"]} is enabled`,
        }),
      ]}
    />

    <PermissionSelect
      permission={{
        name: "insights_supervision_supervisors-list",
        label:
          INSIGHTS_PERMISSIONS_LABELS["insights_supervision_supervisors-list"],
      }}
      disabled={hidePermissions}
      placeholder={
        hidePermissions
          ? undefined
          : routePlaceholder(
              "insights_supervision_supervisors-list",
              selectedUsers
            )
      }
      dependencies={["insights"]}
      rules={[
        ({ getFieldValue }) => ({
          // require an explicit value to ensure no conflict with this field
          required: getFieldValue("insights") === false,
          // this validator checks for the conflict state:
          // supervisors-list cannot be true if insights is false
          validator(rule, value) {
            if (getFieldValue("insights") === false && value !== false) {
              return Promise.reject(
                new Error(
                  `${INSIGHTS_PERMISSIONS_LABELS["insights_supervision_supervisors-list"]} must be False if ${INSIGHTS_PERMISSIONS_LABELS.insights} is False`
                )
              );
            }

            return Promise.resolve();
          },
        }),
      ]}
    />

    <h4>Vitals (Operations):</h4>
    <RoutePermissionList
      labels={VITALS_PERMISSIONS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />

    <h4>Pathways Pages:</h4>
    <RoutePermissionList
      labels={PATHWAYS_PERMISSIONS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />

    <h4>PSI Pages:</h4>
    <RoutePermissionList
      labels={PSI_PERMISSIONS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />

    <h4>Case Planning Assistant (CPA) Pages:</h4>
    <RoutePermissionList
      labels={CPA_PERMISSIONS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />

    <h4>Meeting Mode Pages:</h4>
    <RoutePermissionList
      labels={MEETINGS_PERMISSIONS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />

    <h4>Lantern (Legacy) Pages:</h4>
    <RoutePermissionList
      labels={LANTERN_PERMISSIONS_LABELS}
      hidePermissions={hidePermissions}
      selectedUsers={selectedUsers}
    />
    <hr />
    <h3>Feature Variants:</h3>
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
