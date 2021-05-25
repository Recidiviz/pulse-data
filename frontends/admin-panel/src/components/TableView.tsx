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
import { Breadcrumb, PageHeader, Spin } from "antd";
import * as React from "react";
import { Link, useHistory, useParams } from "react-router-dom";
import { fetchTableNonNullCountsByColumn } from "../AdminPanelAPI";
import useFetchedData from "../hooks";
import MetadataDataset from "../models/MetadataDatasets";
import * as DatasetMetadata from "../navigation/DatasetMetadata";
import MetadataTable from "./MetadataTable";

interface MatchParams {
  table: string;
  dataset: MetadataDataset;
}

const TableView = (): JSX.Element => {
  const { table, dataset: metadataDataset } = useParams<MatchParams>();
  const history = useHistory();

  const fetchValues = React.useCallback(async (): Promise<Response> => {
    return fetchTableNonNullCountsByColumn(metadataDataset, table);
  }, [table, metadataDataset]);
  const { loading, data } = useFetchedData<MetadataAPIResult>(fetchValues);

  if (loading) {
    return (
      <div className="center">
        <Spin size="large" />
      </div>
    );
  }

  const topBreadCrumbLabel =
    DatasetMetadata.getBreadCrumbLabel(metadataDataset);
  const topBreadCrumbRoute =
    DatasetMetadata.routeForMetadataDataset(metadataDataset);

  return (
    <>
      <Breadcrumb>
        <Breadcrumb.Item>
          <Link to={topBreadCrumbRoute}>{topBreadCrumbLabel}</Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>{table}</Breadcrumb.Item>
      </Breadcrumb>
      <PageHeader
        title="Table View"
        onBack={() => {
          history.push(topBreadCrumbRoute);
        }}
      />
      <MetadataTable
        data={data}
        initialColumnTitle="Column"
        initialColumnLink={(name: string) =>
          DatasetMetadata.routeForMetadataColumn(metadataDataset, table, name)
        }
      />
    </>
  );
};

export default TableView;
