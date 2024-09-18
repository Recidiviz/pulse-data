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

import { RawDataResourceLockActor, ResourceLockStatus } from "../constants";

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
        ? `HELD BY MANUAL PROCESS: ${lock.description}`
        : `HELD BY AUTOMATIC PLATFORM PROCESS: ${lock.description}`;
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
        {lockStatusDescription}
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

  lockSecondaryHeaderDescription() {
    return (
      <ul>
        {this.secondaryLocks.length === 0
          ? "NO LOCKS"
          : this.secondaryLocks.map((lock: ResourceLockStatus) => (
              <ResourceLockStatusComponent lock={lock} />
            ))}
      </ul>
    );
  }

  allSecondaryLocksHeldByAdHoc() {
    return this.secondaryLocks.reduce(
      (acc: boolean, lock: ResourceLockStatus) =>
        acc ||
        (lock.actor === RawDataResourceLockActor.ADHOC && !lock.released),
      true
    );
  }

  allSecondaryLocksFree() {
    return this.secondaryLocks.reduce(
      (acc: boolean, lock: ResourceLockStatus) => acc && lock.released,
      true
    );
  }
}
