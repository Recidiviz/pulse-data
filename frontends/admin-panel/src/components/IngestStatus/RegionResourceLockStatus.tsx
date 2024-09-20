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

import { Tooltip } from "antd";

import {
  DirectIngestInstance,
  RawDataResourceLockActor,
  ResourceLockStatus,
} from "./constants";

const ResourceLockStatusComponent = ({
  lock,
}: {
  lock: ResourceLockStatus;
}): JSX.Element => {
  let lockStatusDescription;
  if (lock.released) {
    lockStatusDescription = "FREE";
  } else {
    lockStatusDescription =
      lock.actor === RawDataResourceLockActor.ADHOC
        ? "HELD BY MANUAL PROCESS"
        : "HELD BY AUTOMATIC PLATFORM PROCESS";
  }

  return (
    <div>
      {lock.resource}:{" "}
      <span
        style={{
          color:
            lock.released ||
            (!lock.released && lock.actor !== RawDataResourceLockActor.ADHOC)
              ? "red"
              : "green",
        }}
      >
        <Tooltip title={lock.description}>{lockStatusDescription}</Tooltip>
      </span>
    </div>
  );
};

export class RegionResourceLockStatus {
  primaryLocks: ResourceLockStatus[];

  secondaryLocks: ResourceLockStatus[];

  constructor({
    primaryLocks,
    secondaryLocks,
  }: {
    primaryLocks: ResourceLockStatus[];
    secondaryLocks: ResourceLockStatus[];
  }) {
    this.primaryLocks = primaryLocks;
    this.secondaryLocks = secondaryLocks;
  }

  locksForInstance(rawDataInstance: DirectIngestInstance) {
    if (rawDataInstance === DirectIngestInstance.PRIMARY) {
      return this.primaryLocks;
    }
    if (rawDataInstance === DirectIngestInstance.SECONDARY) {
      return this.secondaryLocks;
    }
    throw new Error(`Unrecognized raw data instance: ${rawDataInstance}`);
  }

  private static allLocksFree(locks: ResourceLockStatus[]) {
    return locks.reduce(
      (acc: boolean, lock: ResourceLockStatus) => acc && lock.released,
      true
    );
  }

  private static allHeldByAdHoc(locks: ResourceLockStatus[]) {
    return locks.reduce(
      (acc: boolean, lock: ResourceLockStatus) =>
        acc ||
        (lock.actor === RawDataResourceLockActor.ADHOC && !lock.released),
      true
    );
  }

  private static lockHeaderDescription(locks: ResourceLockStatus[]) {
    return (
      <ul>
        {locks.length === 0
          ? "NO LOCKS"
          : locks.map((lock: ResourceLockStatus) => (
              <ResourceLockStatusComponent lock={lock} />
            ))}
      </ul>
    );
  }

  secondaryHeaderDescription() {
    return RegionResourceLockStatus.lockHeaderDescription(this.secondaryLocks);
  }

  primaryHeaderDescription() {
    return RegionResourceLockStatus.lockHeaderDescription(this.primaryLocks);
  }

  allSecondaryLocksHeldByAdHoc() {
    return RegionResourceLockStatus.allHeldByAdHoc(this.secondaryLocks);
  }

  allPrimaryLocksHeldByAdHoc() {
    return RegionResourceLockStatus.allHeldByAdHoc(this.primaryLocks);
  }

  allLocksHeldByAdHoc() {
    return (
      this.allPrimaryLocksHeldByAdHoc() && this.allSecondaryLocksHeldByAdHoc()
    );
  }

  allSecondaryLocksFree() {
    return RegionResourceLockStatus.allLocksFree(this.secondaryLocks);
  }

  allPrimaryLocksFree() {
    return RegionResourceLockStatus.allLocksFree(this.primaryLocks);
  }
}
