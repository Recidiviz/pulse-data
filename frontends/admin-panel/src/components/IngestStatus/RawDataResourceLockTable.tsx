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
import { Badge, Table, Tooltip } from "antd";
import { ColumnsType } from "antd/lib/table";
import moment from "moment";
import * as React from "react";

import {
  DirectIngestInstance,
  RawDataResourceLockActor,
  RawDataResourceLockResource,
  ResourceLockMetadata,
  ResourceLockStatus,
} from "./constants";
import { RegionResourceLockStatus } from "./RegionResourceLockStatus";

interface HeldUntilKey {
  lockAcquisitionTime: string;
  ttlSeconds: number;
  released: boolean;
}

interface RawDataResourceLockTableProps {
  rawDataInstance: DirectIngestInstance;
  regionResourceLockStatus: RegionResourceLockStatus | undefined;
  resourceLockMetadata: ResourceLockMetadata | undefined;
  loading: boolean;
}

function renderHeldUntil(record: HeldUntilKey) {
  if (record.released) {
    return null;
  }

  const lockReleaseTime = new Date(record.lockAcquisitionTime);
  lockReleaseTime.setSeconds(lockReleaseTime.getSeconds() + record.ttlSeconds);
  const currentDate = new Date();
  if (lockReleaseTime < currentDate) {
    return null;
  }

  return moment(currentDate).to(lockReleaseTime);
}

const RawDataResourceLockTable: React.FC<RawDataResourceLockTableProps> = ({
  rawDataInstance,
  regionResourceLockStatus,
  resourceLockMetadata,
  loading,
}) => {
  const columns: ColumnsType<ResourceLockStatus> = [
    {
      title: "Resource Lock",
      dataIndex: "resource",
      render: (resource: RawDataResourceLockResource) => (
        <Tooltip title={resourceLockMetadata?.resources[resource]}>
          {resource}
        </Tooltip>
      ),
    },
    {
      title: "Status",
      dataIndex: "released",
      key: "released",
      render: (released: boolean) => (
        <Badge
          status={released ? "success" : "error"}
          text={released ? "FREE" : "HELD"}
        />
      ),
    },
    {
      title: "Lock Expiration Time?",
      dataIndex: ["lockAcquisitionTime", "ttlSeconds", "released"],
      render: (_, rec) => renderHeldUntil(rec),
    },
    {
      title: "Actor",
      dataIndex: ["actor", "released"],
      render: (
        _,
        rec: { actor: RawDataResourceLockActor; released: boolean }
      ) => {
        if (rec.released) return null;
        return (
          <Tooltip title={resourceLockMetadata?.actors[rec.actor]}>
            {rec.actor === RawDataResourceLockActor.ADHOC
              ? "MANUAL"
              : "PLATFORM PROCESS"}
          </Tooltip>
        );
      },
    },
    {
      title: "Description",
      dataIndex: ["description", "released"],
      render: (_, rec: { description: string; released: boolean }) =>
        rec.released ? null : rec.description,
    },
  ];

  return (
    <div>
      {regionResourceLockStatus ? (
        <Table
          columns={columns}
          dataSource={regionResourceLockStatus.locksForInstance(
            rawDataInstance
          )}
          pagination={false}
          loading={loading}
          rowKey="lockId"
        />
      ) : null}
    </div>
  );
};

export default RawDataResourceLockTable;
