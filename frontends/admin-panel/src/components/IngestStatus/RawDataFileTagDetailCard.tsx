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

import { PageHeader } from "antd";
import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import { getLatestRawDataImportRunsForFileTag } from "../../AdminPanelAPI";
import { RawDataFileTagImport } from "./constants";
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

  const getData = useCallback(async () => {
    setImportRunsLoading(true);
    await fetchRawDataImportRuns(stateCode, instance, fileTag);
    setImportRunsLoading(false);
  }, [stateCode, instance, fileTag]);

  useEffect(() => {
    getData();
  }, [getData, stateCode]);

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

  return (
    <>
      <PageHeader title={fileTag} />
      <div
        style={{ height: "90%" }}
        className="main-content content-side-padding"
      >
        <RawDataFileTagRecentRunsTable
          importRuns={importRuns}
          loading={importRunsLoading}
        />
      </div>
    </>
  );
};
export default RawDataFileTagDetail;
