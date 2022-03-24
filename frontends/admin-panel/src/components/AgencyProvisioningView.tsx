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
import { PageHeader, Table } from "antd";
import * as React from "react";
import { getAgencies } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";

type Agency = {
  name: string;
};

type AgencyResponse = {
  agencies: Agency[];
};

const AgencyProvisioningView = (): JSX.Element => {
  const { loading, data } = useFetchedDataJSON<AgencyResponse>(getAgencies);
  const columns = [
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
    },
  ];

  return (
    <>
      <PageHeader title="Agency Provisioning" />
      <Table
        columns={columns}
        dataSource={data?.agencies}
        pagination={{
          hideOnSinglePage: true,
          showSizeChanger: true,
          size: "small",
        }}
        rowKey="state"
      />
    </>
  );
};

export default AgencyProvisioningView;
