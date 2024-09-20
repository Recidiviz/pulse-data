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

import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import {
  getRawDataInstanceLockStatuses,
  getRawDataResourceLockMetadata,
} from "../../AdminPanelAPI/IngestOperations";
import { DirectIngestInstance, ResourceLockMetadata } from "./constants";
import RawDataResourceLockTable from "./RawDataResourceLockTable";
import { RegionResourceLockStatus } from "./RegionResourceLockStatus";

export function getValueIfResolved<Value>(
  result: PromiseSettledResult<Value>
): Value {
  if (result.status === "rejected") {
    throw new Error(result.reason);
  }
  return result.value;
}
const RawDataResourceLockDetail = (): JSX.Element => {
  const { stateCode } = useParams<{ stateCode: string }>();
  const [regionResourceLocks, setRegionResourceLocks] = useState<
    RegionResourceLockStatus | undefined
  >(undefined);
  const [resourceLockMetadata, setResourceLockMetadata] = useState<
    ResourceLockMetadata | undefined
  >(undefined);
  const [loading, setLoading] = useState<boolean>(true);

  const getData = useCallback(async () => {
    setLoading(true);
    const statusResults = await Promise.allSettled([
      getRawDataInstanceLockStatuses(stateCode, DirectIngestInstance.PRIMARY),
      getRawDataInstanceLockStatuses(stateCode, DirectIngestInstance.SECONDARY),
    ]);
    setRegionResourceLocks(
      new RegionResourceLockStatus({
        primaryLocks: await getValueIfResolved(statusResults[0])?.json(),
        secondaryLocks: await getValueIfResolved(statusResults[1])?.json(),
      })
    );
    const metadataResult = await getRawDataResourceLockMetadata();
    setResourceLockMetadata(await metadataResult.json());
    setLoading(false);
  }, [stateCode]);

  useEffect(() => {
    getData();
  }, [getData, stateCode]);

  return (
    <div
      style={{ height: "90%" }}
      className="main-content content-side-padding"
    >
      <RawDataResourceLockTable
        rawDataInstance={DirectIngestInstance.PRIMARY}
        regionResourceLockStatus={regionResourceLocks}
        resourceLockMetadata={resourceLockMetadata}
        loading={loading}
      />
      <RawDataResourceLockTable
        rawDataInstance={DirectIngestInstance.SECONDARY}
        regionResourceLockStatus={regionResourceLocks}
        resourceLockMetadata={resourceLockMetadata}
        loading={loading}
      />
    </div>
  );
};
export default RawDataResourceLockDetail;
