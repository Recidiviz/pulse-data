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
import { Button, PageHeader, Space, message } from "antd";
import { useState } from "react";
import {
  createNewConfiguration,
  getInsightsStateCodes,
} from "../AdminPanelAPI/Insights";
import { AddConfigurationRequest } from "../types";
import { AddConfigForm } from "./Insights/AddConfigForm";
import { checkResponse } from "./StateUserPermissions/utils";
import StateSelector from "./Utilities/StateSelector";
import { StateCodeInfo } from "./general/constants";

const InsightsConfigurationsView = (): JSX.Element => {
  const [stateCode, setStateCode] =
    useState<StateCodeInfo | undefined>(undefined);
  const [addVisible, setAddVisible] = useState(false);

  const onAdd = async (
    request: AddConfigurationRequest,
    stateCodeStr: string
  ) => {
    try {
      const createdConfig = await createNewConfiguration(request, stateCodeStr);
      await checkResponse(createdConfig);
      setAddVisible(false);
      message.success("Configuration added!");
    } catch (err) {
      message.error(`Error adding configuration: ${err}`);
    }
  };

  return (
    <>
      <PageHeader title="Insights Configurations" />
      <Space>
        <StateSelector
          fetchStateList={getInsightsStateCodes}
          onChange={(state) => setStateCode(state)}
        />
        <Button
          onClick={() => {
            setAddVisible(true);
          }}
          disabled={!stateCode}
        >
          Create new version
        </Button>
        {stateCode?.code && (
          <AddConfigForm
            stateCode={stateCode?.code}
            addVisible={addVisible}
            addOnCancel={() => setAddVisible(false)}
            addOnSubmit={onAdd}
          />
        )}
      </Space>
    </>
  );
};

export default InsightsConfigurationsView;
