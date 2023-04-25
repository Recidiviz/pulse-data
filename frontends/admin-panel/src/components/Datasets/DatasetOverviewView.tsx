// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2022 Recidiviz, Inc.
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
import { useParams } from "react-router-dom";
import { fetchObjectCountsByTable } from "../../AdminPanelAPI";
import { useFetchedDataJSON } from "../../hooks";
import MetadataDataset from "../../models/MetadataDatasets";
import * as DatasetMetadata from "../../navigation/DatasetMetadata";
import { MetadataAPIResult } from "../../types";
import MetadataTable from "../MetadataTable";
import DatasetsHeader from "./DatasetsHeader";

interface MatchParams {
  dataset: MetadataDataset;
}

interface DatesetOverviewViewProps {
  stateCode: string | null;
}

const DatasetOverviewView: React.FC<DatesetOverviewViewProps> = ({
  stateCode,
}) => {
  const { dataset: metadataDataset } = useParams<MatchParams>();

  const fetchValues = useCallback(async (): Promise<Response> => {
    return fetchObjectCountsByTable(metadataDataset);
  }, [metadataDataset]);
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

  return (
    <>
      <DatasetsHeader
        title="Dataset View"
        stateCode={stateCode}
        breadCrumbLinks={[topBreadCrumbLabel]}
      />
      <Layout className="content-side-padding">
        <MetadataTable
          data={data}
          stateCode={stateCode}
          initialColumnTitle="Tables"
          initialColumnLink={(name: string) =>
            DatasetMetadata.routeForMetadataTable(metadataDataset, name)
          }
        />
      </Layout>
    </>
  );
};

export default DatasetOverviewView;
