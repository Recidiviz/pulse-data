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

import { Alert, Breadcrumb, Layout } from "antd";
import { FC } from "react";
import { Link, useParams } from "react-router-dom";
import {
  addStateCodeQueryToLink,
  VALIDATION_STATUS_ROUTE,
} from "../../navigation/DatasetMetadata";
import StateSelectorPageHeader from "../general/StateSelectorPageHeader";
import { StateCodeInfo } from "../IngestOperationsView/constants";
import ValidationDetails from "./ValidationDetails";

interface MatchParams {
  validationName: string;
}

interface ValidationDetailViewProps {
  stateCode: string | null;
  stateCodeChange: (value: StateCodeInfo) => void;
}

const ValidationDetailView: FC<ValidationDetailViewProps> = ({
  stateCode,
  stateCodeChange,
}) => {
  const { validationName } = useParams<MatchParams>();

  return (
    <>
      <StateSelectorPageHeader
        title="Validation Details"
        subTitle="Shows the detailed status for the validation in this state."
        stateCode={stateCode}
        onChange={stateCodeChange}
      />
      <Layout className="content-side-padding">
        <Breadcrumb>
          <Breadcrumb.Item>
            <Link
              to={addStateCodeQueryToLink(VALIDATION_STATUS_ROUTE, stateCode)}
            >
              Validation Status
            </Link>
          </Breadcrumb.Item>
          <Breadcrumb.Item>{validationName}</Breadcrumb.Item>
        </Breadcrumb>
        {stateCode ? (
          <ValidationDetails
            validationName={validationName}
            stateCode={stateCode}
          />
        ) : (
          <Alert
            message="A region must be selected to view the below information"
            type="warning"
            showIcon
          />
        )}
      </Layout>
    </>
  );
};

export default ValidationDetailView;
