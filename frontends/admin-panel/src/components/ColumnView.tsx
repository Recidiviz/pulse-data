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
import { Link, useHistory, useParams } from "react-router-dom";

import MetadataTable from "./MetadataTable";

import * as IngestMetadata from "../navigation/IngestMetadata";
import { fetchColumnObjectCountsByValue } from "../AdminPanelAPI";
import useFetchedData from "../hooks";

interface MatchParams {
  column: string;
  table: string;
}

const ColumnView = (): JSX.Element => {
  const { column, table } = useParams<MatchParams>();
  const history = useHistory();

  const fetchValues = React.useCallback(async () => {
    return fetchColumnObjectCountsByValue(table, column);
  }, [table, column]);
  const { loading, data } = useFetchedData(fetchValues);

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
        <Breadcrumb.Item>
          <Link to={IngestMetadata.ROOT_ROUTE}>state</Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>
          <Link to={IngestMetadata.routeForTable(table)}>{table}</Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>{column}</Breadcrumb.Item>
      </Breadcrumb>
      <PageHeader
        title="Column Breakdown"
        onBack={() => {
          history.push(IngestMetadata.routeForTable(table));
        }}
      />
      <MetadataTable data={data} initialColumnTitle="Value" />
    </>
  );
};

export default ColumnView;
