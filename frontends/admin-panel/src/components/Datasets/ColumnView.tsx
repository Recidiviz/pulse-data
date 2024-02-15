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

import { fetchColumnObjectCountsByValue } from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import MetadataDataset from "../../models/MetadataDatasets";
import * as DatasetMetadata from "../../navigation/DatasetMetadata";
import { MetadataAPIResult } from "../../types";
import MetadataTable from "../MetadataTable";
import DatasetsHeader from "./DatasetsHeader";

interface MatchParams {
  column: string;
  table: string;
  dataset: MetadataDataset;
}

interface ColumnViewProps {
  stateCode: string | null;
}

const ColumnView: React.FC<ColumnViewProps> = ({ stateCode }) => {
  const { column, table, dataset: metadataDataset } = useParams<MatchParams>();
  const history = useHistory();

  const fetchValues = useCallback(async () => {
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
  const topBreadCrumbRoute = DatasetMetadata.addStateCodeQueryToLink(
    DatasetMetadata.routeForMetadataDataset(metadataDataset),
    stateCode
  );
  const secondBreadCrumbRoute = DatasetMetadata.addStateCodeQueryToLink(
    DatasetMetadata.routeForMetadataTable(metadataDataset, table),
    stateCode
  );

  return (
    <>
      <DatasetsHeader
        title="Column Breakdown"
        stateCode={stateCode}
        backAction={() => {
          history.push(secondBreadCrumbRoute);
        }}
        breadCrumbLinks={[
          <Link to={topBreadCrumbRoute}>{topBreadCrumbLabel}</Link>,
          <Link to={secondBreadCrumbRoute}>{table}</Link>,
          column,
        ]}
      />
      <Layout className="content-side-padding">
        <MetadataTable
          stateCode={stateCode}
          data={data}
          initialColumnTitle="Value"
        />
      </Layout>
    </>
  );
};

export default ColumnView;
