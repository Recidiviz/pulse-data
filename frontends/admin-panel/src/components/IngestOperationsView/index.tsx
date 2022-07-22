// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { Alert, PageHeader } from "antd";
import * as React from "react";
import { useHistory, useLocation } from "react-router-dom";
import { fetchIngestStateCodes } from "../../AdminPanelAPI";
import StateSelector from "../Utilities/StateSelector";
import { StateCodeInfo } from "./constants";
import StateSpecificIngestOperationsMetadata from "./StateSpecificIngestOperationsMetadata";

const IngestOperationsView = (): JSX.Element => {
  const history = useHistory();

  const location = useLocation();
  const queryString = location.search;
  const params = new URLSearchParams(queryString);
  const initialStateCode = params.get("stateCode");

  const [stateCode, setStateCode] =
    React.useState<string | null>(initialStateCode);

  const stateCodeChange = (value: StateCodeInfo) => {
    setStateCode(value.code);
    updateQueryParams(value.code);
  };

  const updateQueryParams = (stateCodeInput: string) => {
    const locationObj = {
      search: `?stateCode=${stateCodeInput}`,
    };
    history.push(locationObj);
  };

  return (
    <>
      <PageHeader
        title="Ingest Operations"
        extra={[
          <StateSelector
            fetchStateList={fetchIngestStateCodes}
            onChange={stateCodeChange}
            initialValue={initialStateCode}
          />,
        ]}
      />
      {stateCode ? (
        <StateSpecificIngestOperationsMetadata stateCode={stateCode} />
      ) : (
        <Alert
          message="A region must be selected to view the below information"
          type="warning"
          showIcon
        />
      )}
    </>
  );
};
export default IngestOperationsView;
