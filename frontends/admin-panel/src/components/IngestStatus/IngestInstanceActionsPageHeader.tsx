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

import { PageHeader, Popover } from "antd";
import classNames from "classnames";
import { useEffect, useState } from "react";

import { getCurrentIngestInstanceStatusInformation } from "../../AdminPanelAPI/IngestOperations";
import {
  RegionAction,
  regionActionNames,
} from "../Utilities/ActionRegionConfirmationForm";
import { DirectIngestInstance, IngestInstanceStatusInfo } from "./constants";
import IngestActionButton from "./IngestActionButton";
import {
  getLegacyIngestStatusBoxColor,
  getLegacyIngestStatusMessage,
  removeUnderscore,
} from "./ingestStatusUtils";

interface IngestActionsPageHeaderProps {
  stateCode: string;
  instance: DirectIngestInstance;
}

const IngestInstanceActionsPageHeader: React.FC<
  IngestActionsPageHeaderProps
> = ({ stateCode, instance }) => {
  const [statusInfo, setStatusInfo] = useState<
    IngestInstanceStatusInfo | undefined
  >();

  useEffect(() => {
    const setCurrentIngestInstanceStatusInformation = async () => {
      const response = await getCurrentIngestInstanceStatusInformation(
        stateCode,
        instance
      );
      const newStatusInfo = (await response.json()) as IngestInstanceStatusInfo;
      setStatusInfo(newStatusInfo);
    };
    setCurrentIngestInstanceStatusInformation();
  }, [stateCode, instance]);

  const IngestInstanceStatusPopoverContent = (
    <div>
      {statusInfo
        ? getLegacyIngestStatusMessage(
            statusInfo.status,
            statusInfo.statusTimestamp
          )
        : null}
    </div>
  );

  return (
    <PageHeader
      title={instance}
      tags={
        statusInfo
          ? [
              <Popover content={IngestInstanceStatusPopoverContent}>
                <div
                  className={classNames(
                    "tag",
                    "tag-with-color",
                    getLegacyIngestStatusBoxColor(statusInfo.status)
                  )}
                >
                  {removeUnderscore(statusInfo.status)}
                </div>
              </Popover>,
            ]
          : []
      }
      extra={
        <>
          <IngestActionButton
            style={{ marginRight: 5 }}
            action={RegionAction.TriggerTaskScheduler}
            buttonText={regionActionNames[RegionAction.TriggerTaskScheduler]}
            instance={instance}
            stateCode={stateCode}
            type="primary"
          />
          <IngestActionButton
            style={
              // TODO(#13406) Remove check if rerun button should be present for PRIMARY as well.
              instance === "SECONDARY"
                ? { marginRight: 5 }
                : { display: "none" }
            }
            action={RegionAction.StartRawDataReimport}
            buttonText={regionActionNames[RegionAction.StartRawDataReimport]}
            instance={instance}
            stateCode={stateCode}
            type="primary"
          />
        </>
      }
    />
  );
};

export default IngestInstanceActionsPageHeader;
