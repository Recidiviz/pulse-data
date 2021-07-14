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
import { fetchDataFreshness } from "../../AdminPanelAPI";
import useFetchedData from "../../hooks";

interface DataFreshnessInfoProps {
  state: string;
}

const DataFreshnessInfo: React.FC<DataFreshnessInfoProps> = ({ state }) => {
  const isProduction = window.RUNTIME_GCP_ENVIRONMENT === "production";
  const projectId = isProduction ? "recidiviz-123" : "recidiviz-staging";

  const { data } = useFetchedData<DataFreshnessResult[]>(fetchDataFreshness);

  function searchForState() {
    if (data !== undefined) {
      for (let i = 0; i < data.length; i += 1) {
        if (data[i].state === state) {
          return data[i];
        }
      }
    }
  }

  const showLink = (
    <a
      href={`https://${projectId}.ue.r.appspot.com/admin/ingest_metadata/data_freshness`}
    >
      here
    </a>
  );

  const stateData = searchForState();
  if (stateData?.ingestPaused) {
    return (
      <p style={{ textAlign: "center" }}>
        Data Freshness:
        <span style={{ color: "red" }}> Warning! </span>
        BigQuery data refreshes have been paused. Data may be incorrect. More
        info {showLink}.
      </p>
    );
  }
  if (stateData?.date) {
    return (
      <p style={{ textAlign: "center" }}>
        Data Freshness:
        <span style={{ color: "green" }}> Fresh! </span>
        Data is up to date and current as of {stateData.date}. More info{" "}
        {showLink}.
      </p>
    );
  }
  return (
    <p style={{ textAlign: "center" }}>
      Data Freshness:
      <span style={{ color: "red" }}> Warning! </span> Date is null. More info{" "}
      {showLink}.
    </p>
  );
};

export default DataFreshnessInfo;
