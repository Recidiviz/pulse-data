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
import { Layout, Spin } from "antd";
import { useCallback } from "react";
import { Link, useHistory, useParams } from "react-router-dom";

import { fetchTableNonNullCountsByColumn } from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import MetadataDataset from "../../models/MetadataDatasets";
import * as DatasetMetadata from "../../navigation/DatasetMetadata";
import { MetadataAPIResult } from "../../types";
import MetadataTable from "../MetadataTable";
import DatasetsHeader from "./DatasetsHeader";

interface MatchParams {
  table: string;
  dataset: MetadataDataset;
}

interface TableViewProps {
  stateCode: string | null;
}

const TableView: React.FC<TableViewProps> = ({ stateCode }) => {
  const { table, dataset: metadataDataset } = useParams<MatchParams>();
  const history = useHistory();

  const fetchValues = useCallback(async (): Promise<Response> => {
    return fetchTableNonNullCountsByColumn(metadataDataset, table);
  }, [table, metadataDataset]);
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
  const topBreadCrumbRoute = DatasetMetadata.addStateCodeQueryToLink(
    DatasetMetadata.routeForMetadataDataset(metadataDataset),
    stateCode
  );

  return (
    <>
      <DatasetsHeader
        title="Table View"
        stateCode={stateCode}
        backAction={() => {
          history.push(topBreadCrumbRoute);
        }}
        breadCrumbLinks={[
          <Link to={topBreadCrumbRoute}>{topBreadCrumbLabel}</Link>,
          table,
        ]}
      />
      <Layout className="content-side-padding">
        <MetadataTable
          data={data}
          stateCode={stateCode}
          initialColumnTitle="Column"
          initialColumnLink={(name: string) =>
            DatasetMetadata.routeForMetadataColumn(metadataDataset, table, name)
          }
        />
      </Layout>
    </>
  );
};

export default TableView;
