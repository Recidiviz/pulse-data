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

import { SyncOutlined } from "@ant-design/icons";
import { Button, PageHeader, Tooltip } from "antd";
import classNames from "classnames";

import {
  DirectIngestInstance,
  resourceLockHeldStates,
  ResourceLockState,
  ResourceLockStatus,
} from "./constants";
import {
  getResourceLockAdHocCumulativeState,
  getResourceLockColor,
  removeUnderscore,
} from "./ingestStatusUtils";
import LockActionButton, {
  LockAction,
} from "./RawDataResourceLockActionButton";

const ResourceLockActionButtonTooltipFromState: {
  [state in ResourceLockState]: {
    acquireTooltip: string;
    releaseTooltip: string;
  };
} = {
  ADHOC_HELD: {
    acquireTooltip:
      "You cannot acquire resource locks as you already have them!",
    releaseTooltip:
      "If you want to relinquish sole read/write access to the listed raw data resources to allow platform processes (i.e. raw data import, sftp (eventually ingest dag)) to access them, release the adhoc/manual hold on the locks.",
  },
  PROCESS_HELD: {
    acquireTooltip:
      "You cannot acquire locks while they are being held by a platform process; please wait until the process is completed using the resource.",
    releaseTooltip:
      "CAUTION: A platform process has sole read/write access to the listed raw data resources, releasing them may have unintended side effects. Please only proceed if you are sure that the process has completed without releasing locks successfully.",
  },
  FREE: {
    acquireTooltip:
      "If you want prevent platform processes from running (i.e. raw data import, sftp (eventually ingest dag)), acquire the resource locks to be granted sole read/write access to them.",
    releaseTooltip: "You cannot release locks that are already free.",
  },
  MIXED: {
    acquireTooltip:
      "You cannot acquire all resource locks when some are that are held.",
    releaseTooltip:
      "CAUTION: Locks are held by both ad-hoc/manual and platform process actors. If a platform process has sole read/write access to the listed raw data resources, releasing them may have unintended side effects. Please only proceed if you are sure that the process has completed without releasing locks successfully.",
  },
  UNKNOWN: {
    acquireTooltip:
      "You cannot acquire resource locks that are in an unknown state.",
    releaseTooltip: "You cannot release locks that are in an unknown state.",
  },
};

interface RawDataResourceLockActionTableHeaderProps {
  stateCode: string;
  rawDataInstance: DirectIngestInstance;
  lockStatus: ResourceLockStatus[];
  onRefreshResourceLockData: () => void;
}

const RawDataResourceLockActionTableHeader: React.FC<
  RawDataResourceLockActionTableHeaderProps
> = ({ stateCode, rawDataInstance, lockStatus, onRefreshResourceLockData }) => {
  const lockState = getResourceLockAdHocCumulativeState(lockStatus);
  const { acquireTooltip, releaseTooltip } =
    ResourceLockActionButtonTooltipFromState[lockState];
  return (
    <PageHeader
      title={`${rawDataInstance} Lock State`}
      tags={createRawDataResourceLockStatusTag(lockState)}
      extra={
        <>
          <Tooltip title={acquireTooltip}>
            <div>
              <LockActionButton
                style={{ display: "block", textAlign: "center", width: "auto" }}
                action={LockAction.AcquireRawDataResourceLocks}
                disabled={lockState !== ResourceLockState.FREE} // only enabled when all locks are FREE
                stateCode={stateCode}
                rawDataInstance={rawDataInstance}
                buttonText={`${LockAction.AcquireRawDataResourceLocks} for ${rawDataInstance}`}
                onActionConfirmed={() => {
                  onRefreshResourceLockData();
                }}
                currentLocks={lockStatus}
                key={LockAction.AcquireRawDataResourceLocks}
                lockState={lockState}
              />
            </div>
          </Tooltip>
          <Tooltip title={releaseTooltip}>
            <div>
              <LockActionButton
                style={{ display: "block", textAlign: "center", width: "auto" }}
                action={LockAction.ReleaseRawDataResourceLocks}
                disabled={!resourceLockHeldStates.has(lockState)} // enabled when any kind of lock is held
                stateCode={stateCode}
                rawDataInstance={rawDataInstance}
                buttonText={`${LockAction.ReleaseRawDataResourceLocks} for ${rawDataInstance}`}
                onActionConfirmed={() => {
                  onRefreshResourceLockData();
                }}
                currentLocks={lockStatus}
                key={LockAction.ReleaseRawDataResourceLocks}
                lockState={lockState}
              />
            </div>
          </Tooltip>
          <Button
            type="primary"
            shape="circle"
            icon={<SyncOutlined />}
            onClick={() => onRefreshResourceLockData()}
          />
        </>
      }
    />
  );
};

export default RawDataResourceLockActionTableHeader;

function createRawDataResourceLockStatusTag(
  lockState: ResourceLockState
): JSX.Element {
  const lockStateStr = lockState.toString();
  return (
    <div
      className={classNames(
        "tag",
        "tag-with-color",
        getResourceLockColor(lockStateStr)
      )}
    >
      {removeUnderscore(lockStateStr)}
    </div>
  );
}
