// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { Breadcrumb, Layout } from "antd";
import { useCallback, useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { getLatestRawDataImportRunsForFileTag } from "../../AdminPanelAPI";
import { getRawFileConfigSummary } from "../../AdminPanelAPI/IngestOperations";
import {
  addStateCodeAndInstanceToLink,
  INGEST_DATAFLOW_INSTANCE_ROUTE,
} from "../../navigation/IngestOperations";
import { RawDataFileTagImport, RawFileConfigSummary } from "./constants";
import RawFileConfigTable from "./RawDataFileTagConfigTable";
import RawDataFileTagRecentRunsTable from "./RawDataFileTagRecentRunsTable";

interface RawDataFileTagDetailParams {
  stateCode: string;
  instance: string;
  fileTag: string;
}

const RawDataFileTagDetail = (): JSX.Element => {
  const { stateCode, instance, fileTag } =
    useParams<RawDataFileTagDetailParams>();
  const [importRuns, setImportRuns] = useState<RawDataFileTagImport[]>([]);
  const [importRunsLoading, setImportRunsLoading] = useState<boolean>(true);
  const [rawFileConfig, setRawFileConfig] = useState<
    RawFileConfigSummary | undefined
  >(undefined);
  const [rawFileConfigLoading, setRawFileConfigLoading] =
    useState<boolean>(true);

  const getImportRuns = useCallback(async () => {
    setImportRunsLoading(true);
    await fetchRawDataImportRuns(stateCode, instance, fileTag);
    setImportRunsLoading(false);
  }, [stateCode, instance, fileTag]);

  const getRawFileConfig = useCallback(async () => {
    setRawFileConfigLoading(true);
    await fetchRawFileConfig(stateCode, instance, fileTag);
    setRawFileConfigLoading(false);
  }, [stateCode, instance, fileTag]);

  useEffect(() => {
    getImportRuns();
    getRawFileConfig();
  }, [getImportRuns, getRawFileConfig, stateCode]);

  async function fetchRawDataImportRuns(
    fetchableStateCode: string,
    fetchableRawDataInstance: string,
    fetchableFileTag: string
  ) {
    const response = await getLatestRawDataImportRunsForFileTag(
      fetchableStateCode,
      fetchableRawDataInstance,
      fetchableFileTag
    );
    const result: RawDataFileTagImport[] = await response.json();
    setImportRuns(result);
  }

  async function fetchRawFileConfig(
    fetchableStateCode: string,
    fetchableInstance: string,
    fetchableFileTag: string
  ) {
    const response = await getRawFileConfigSummary(
      fetchableStateCode,
      fetchableInstance,
      fetchableFileTag
    );
    const result: RawFileConfigSummary | undefined = await response.json();
    setRawFileConfig(result);
  }

  return (
    <Layout className="main-content content-side-padding">
      <Breadcrumb>
        <Breadcrumb.Item>
          <Link
            to={addStateCodeAndInstanceToLink(
              INGEST_DATAFLOW_INSTANCE_ROUTE,
              stateCode,
              instance
            )}
          >
            {stateCode} Raw Data
          </Link>
        </Breadcrumb.Item>
        <Breadcrumb.Item>{fileTag}</Breadcrumb.Item>
      </Breadcrumb>
      <RawFileConfigTable
        rawFileConfigSummary={rawFileConfig}
        stateCode={stateCode}
        loading={rawFileConfigLoading}
      />
      <RawDataFileTagRecentRunsTable
        importRuns={importRuns}
        loading={importRunsLoading}
      />
    </Layout>
  );
};
export default RawDataFileTagDetail;
