// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2020 Recidiviz, Inc.
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
import { Breadcrumb, PageHeader, Spin } from "antd";

import MetadataTable from "./MetadataTable";

import * as IngestMetadata from "../navigation/IngestMetadata";
import { fetchObjectCountsByTable } from "../AdminPanelAPI";
import useFetchedData from "../hooks";

const DatasetView = (): JSX.Element => {
  const { loading, data } = useFetchedData<MetadataAPIResult>(
    fetchObjectCountsByTable
  );

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  return (
    <>
      <Breadcrumb>
        <Breadcrumb.Item>state</Breadcrumb.Item>
      </Breadcrumb>
      <PageHeader title="Dataset View" />
      <MetadataTable
        data={data}
        initialColumnTitle="Tables"
        initialColumnLink={(name: string) => IngestMetadata.routeForTable(name)}
      />
    </>
  );
};

export default DatasetView;
