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

import { PageHeader, Popover, Tag } from "antd";
import { useEffect, useState } from "react";
import {
  RegionAction,
  regionActionNames,
} from "../../Utilities/ActionRegionConfirmationForm";
import { fetchCurrentIngestInstanceStatus } from "../../Utilities/IngestInstanceUtilities";
import { DirectIngestInstance, IngestInstanceSummary } from "../constants";
import { getStatusMessage, removeUnderscore } from "../ingestStatusUtils";
import IngestActionButton from "./IngestActionButton";

interface IngestActionsPageHeaderProps {
  stateCode: string;
  instance: DirectIngestInstance;
  ingestInstanceSummary: IngestInstanceSummary | undefined;
  onRefreshIngestSummary: () => void;
}

const IngestInstanceActionsPageHeader: React.FC<IngestActionsPageHeaderProps> =
  ({ stateCode, instance, ingestInstanceSummary, onRefreshIngestSummary }) => {
    const pauseUnpauseInstanceAction = ingestInstanceSummary?.operations
      .isPaused
      ? RegionAction.UnpauseIngestInstance
      : RegionAction.PauseIngestInstance;

    const [ingestInstanceStatus, setIngestInstanceStatus] =
      useState<string | undefined>(undefined);

    useEffect(() => {
      fetchCurrentIngestInstanceStatus(stateCode, instance).then((value) =>
        setIngestInstanceStatus(value)
      );
    }, [stateCode, instance]);

    const IngestInstanceStatusPopoverContent = (
      <div>
        {ingestInstanceStatus ? getStatusMessage(ingestInstanceStatus) : null}
      </div>
    );

    return (
      <PageHeader
        title={instance}
        tags={
          ingestInstanceStatus ? (
            <Popover content={IngestInstanceStatusPopoverContent}>
              {/* TODO(#15717): Match tag color to status color in summary page */}
              <Tag color="blue">{removeUnderscore(ingestInstanceStatus)}</Tag>
            </Popover>
          ) : (
            <></>
          )
        }
        extra={
          ingestInstanceSummary ? (
            <>
              <IngestActionButton
                style={{ marginRight: 5 }}
                action={RegionAction.ExportToGCS}
                buttonText={regionActionNames[RegionAction.ExportToGCS]}
                instance={instance}
                stateCode={stateCode}
              />
              <IngestActionButton
                style={
                  ingestInstanceSummary.operations.isPaused
                    ? { display: "none" }
                    : { marginRight: 5 }
                }
                action={RegionAction.TriggerTaskScheduler}
                buttonText={
                  regionActionNames[RegionAction.TriggerTaskScheduler]
                }
                instance={instance}
                stateCode={stateCode}
              />
              <IngestActionButton
                style={
                  // TODO(#13406) Remove check if rerun button should be present for PRIMARY as well.
                  instance === "SECONDARY"
                    ? { marginRight: 5 }
                    : { display: "none" }
                }
                action={RegionAction.StartIngestRerun}
                buttonText={regionActionNames[RegionAction.StartIngestRerun]}
                instance={instance}
                stateCode={stateCode}
              />
              <IngestActionButton
                action={pauseUnpauseInstanceAction}
                buttonText={regionActionNames[pauseUnpauseInstanceAction]}
                instance={instance}
                stateCode={stateCode}
                onActionConfirmed={() => {
                  onRefreshIngestSummary();
                }}
                type="primary"
              />
            </>
          ) : (
            <></>
          )
        }
      />
    );
  };

export default IngestInstanceActionsPageHeader;
