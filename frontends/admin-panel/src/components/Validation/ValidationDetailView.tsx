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

import { Alert, Breadcrumb, PageHeader } from "antd";
import * as React from "react";
import { Link, useHistory, useLocation, useParams } from "react-router-dom";
import { fetchValidationStateCodes } from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import { VALIDATION_STATUS_ROUTE } from "../../navigation/DatasetMetadata";
import { StateCodeInfo } from "../IngestOperationsView/constants";
import StateSelector from "../Utilities/StateSelector";
import ValidationDetails from "./ValidationDetails";

interface MatchParams {
  validationName: string;
}

const ValidationDetailView = (): JSX.Element => {
  const { validationName } = useParams<MatchParams>();

  const history = useHistory();

  const location = useLocation();
  const queryString = location.search;
  const params = new URLSearchParams(queryString);

  const stateCodesFetched = useFetchedDataJSON<StateCodeInfo[]>(
    fetchValidationStateCodes
  );
  const stateCodesLoading = stateCodesFetched.loading;
  const stateCodesData = stateCodesFetched.data;

  const updateQueryParams = (stateCodeInput: string) => {
    const locationObj = {
      search: `?stateCode=${stateCodeInput}`,
    };
    history.push(locationObj);
  };
  const state = stateCodesData?.find(
    (value: StateCodeInfo) => value.code === params.get("stateCode")
  );
  const updateState = (value: StateCodeInfo) => {
    updateQueryParams(value.code);
  };

  return (
    <>
      <Breadcrumb>
        <Breadcrumb.Item>
          <Link to={VALIDATION_STATUS_ROUTE}>Validation Status</Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>{validationName}</Breadcrumb.Item>
      </Breadcrumb>
      <PageHeader
        title="Validation Details"
        subTitle="Shows the detailed status for the validation in this state."
        extra={
          <StateSelector
            onChange={updateState}
            value={state?.code}
            loading={stateCodesLoading}
            data={stateCodesData}
          />
        }
      />
      {state ? (
        <ValidationDetails validationName={validationName} stateInfo={state} />
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

export default ValidationDetailView;
