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
import * as React from "react";
import { PageHeader, Spin, Table } from "antd";
import { getPOFeedback } from "../AdminPanelAPI";
import useFetchedData from "../hooks";

const POFeedbackView = (): JSX.Element => {
  const { loading, data } = useFetchedData<POFeedbackResponse[]>(getPOFeedback);

  if (loading || !data) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const sortedData = data.sort((a, b) => (a.timestamp < b.timestamp ? 1 : -1));

  const columns = [
    {
      title: "Officer ID",
      key: "officerExternalId",
      dataIndex: "officerExternalId",
    },
    {
      title: "Person ID",
      key: "personExternalId",
      dataIndex: "personExternalId",
    },
    {
      title: "Other Text",
      key: "otherText",
      dataIndex: "otherText",
    },
    {
      title: "Timestamp",
      key: "timestamp",
      dataIndex: "timestamp",
    },
  ];

  return (
    <>
      <PageHeader title="PO Feedback" />
      <Table dataSource={sortedData} columns={columns} />
    </>
  );
};

export default POFeedbackView;
