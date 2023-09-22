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

import { Button, PageHeader, Select, Spin, message } from "antd";
import React, { useState } from "react";
import {
  deleteDemoClientUpdatesV2,
  getStateRoleDefaultPermissions,
} from "../../AdminPanelAPI/LineStaffTools";
import { useFetchedDataJSON } from "../../hooks";
import { StateRolePermissionsResponse } from "../../types";
import { EnvironmentType } from "../types";

const DemoAppManagementView = (): JSX.Element => {
  const { loading, data } = useFetchedDataJSON<StateRolePermissionsResponse[]>(
    getStateRoleDefaultPermissions
  );
  const [selectedStateCode, setSelectedStateCode] =
    useState<string | undefined>();
  const [messageApi, contextHolder] = message.useMessage();

  const env = (window.RUNTIME_GCP_ENVIRONMENT ||
    "production") as EnvironmentType;

  const disableReset = env === "production";

  if (loading || !data) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const handleResetButtonClick = async () => {
    const response = await deleteDemoClientUpdatesV2(selectedStateCode);

    if (response.status === 200) {
      messageApi.open({
        type: "success",
        content: "Successfully reset demo client update data.",
      });
    } else {
      messageApi.open({
        type: "error",
        content:
          "There was an error resetting the demo client update data. Please retry",
      });
    }

    setSelectedStateCode(undefined);
  };

  const stateCodes = data
    .map((d) => d.stateCode)
    .filter((v, i, a) => a.indexOf(v) === i);

  return (
    <>
      <PageHeader title="Demo App Management" />
      <h3>Reset client/resident updates data</h3>
      <p>
        <span>
          Clears the client/resident update data in the Demo App such that demos
          can be given with a fresh set of data.
        </span>
        <br />
        <br />
        <span>
          Select a state code to clear only data for that state. If no state
          code is selected, data for all states will be reset.
        </span>
        {disableReset && (
          <>
            <br />
            <br />
            <span>
              Client updates reset is not enabled in the Production app. Please
              navigate to the staging app.
            </span>
          </>
        )}
      </p>
      <Select
        options={stateCodes.map((r) => {
          return { value: r, label: r };
        })}
        placeholder="State code"
        style={{ minWidth: "75px", marginRight: "15px" }}
        onSelect={(value: string) => setSelectedStateCode(value)}
        disabled={disableReset}
      />
      {contextHolder}
      <Button onClick={handleResetButtonClick} disabled={disableReset}>
        Reset
      </Button>
    </>
  );
};

export default DemoAppManagementView;
