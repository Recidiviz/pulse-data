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
import { fetchColumnObjectCountsByValue } from "../AdminPanelAPI";
import { useFetchedDataJSON } from "../hooks";
import MetadataDataset from "../models/MetadataDatasets";
import * as DatasetMetadata from "../navigation/DatasetMetadata";
import MetadataTable from "./MetadataTable";

interface MatchParams {
  column: string;
  table: string;
  dataset: MetadataDataset;
}

const ColumnView = (): JSX.Element => {
  const { column, table, dataset: metadataDataset } = useParams<MatchParams>();
  const history = useHistory();

  const fetchValues = React.useCallback(async () => {
    return fetchColumnObjectCountsByValue(metadataDataset, table, column);
  }, [table, column, metadataDataset]);
  const { loading, data } = useFetchedDataJSON<MetadataAPIResult>(fetchValues);

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
  const secondBreadCrumbRoute = DatasetMetadata.routeForMetadataTable(
    metadataDataset,
    table
  );

  return (
    <>
      <Breadcrumb>
        <Breadcrumb.Item>
          <Link to={topBreadCrumbRoute}>{topBreadCrumbLabel}</Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>
          <Link to={secondBreadCrumbRoute}>{table}</Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>{column}</Breadcrumb.Item>
      </Breadcrumb>
      <PageHeader
        title="Column Breakdown"
        onBack={() => {
          history.push(
            DatasetMetadata.routeForMetadataTable(metadataDataset, table)
          );
        }}
      />
      <MetadataTable data={data} initialColumnTitle="Value" />
    </>
  );
};

export default ColumnView;
