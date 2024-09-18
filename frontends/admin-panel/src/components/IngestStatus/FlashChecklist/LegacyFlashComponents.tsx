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
import { Alert, Button } from "antd";
import { observer } from "mobx-react-lite";
import React from "react";

import { useLegacyFlashChecklistStore } from "./FlashChecklistStore";
import { runAndCheckStatus } from "./FlashUtils";
import { LegacyCurrentRawDataInstanceStatus } from "./LegacyCurrentRawDataInstanceStatus";

export const LegacyCannotFlashDecisionWrongStatusComponent = ({
  currentIngestStatus,
  onSelectProceed,
}: {
  currentIngestStatus: LegacyCurrentRawDataInstanceStatus;
  onSelectProceed: () => void;
}): JSX.Element => {
  const cannotFlashDescription = `Primary: ${currentIngestStatus.primary}. Secondary: ${currentIngestStatus.secondary}.`;
  return (
    <div>
      <Alert
        message="Cannot proceed with flash to primary. Secondary instance needs to have the status 'READY_TO_FLASH'."
        description={cannotFlashDescription}
        type="info"
        showIcon
      />
      <br />
      <h3 style={{ color: "green" }}>
        Regardless of ingest instance status, you may proceed with cleaning up
        the secondary instance and canceling the rerun:{" "}
        <Button type="primary" onClick={onSelectProceed}>
          CLEAN UP SECONDARY + CANCEL RAW DATA REIMPORT
        </Button>
      </h3>
    </div>
  );
};

export const LegacyCannotFlashDecisionNonEmptyBucketComponent = ({
  onSelectProceed,
}: {
  onSelectProceed: () => void;
}): JSX.Element => {
  const { projectId, stateInfo, currentRawFileProcessingStatus } =
    useLegacyFlashChecklistStore();

  const formattedStateCode = stateInfo?.code.toLowerCase().replaceAll("_", "-");
  const primaryBucketURL = `https://console.cloud.google.com/storage/browser/${projectId}-direct-ingest-state-${formattedStateCode}`;
  const secondaryBucketURL = `https://console.cloud.google.com/storage/browser/${projectId}-direct-ingest-state-${formattedStateCode}-secondary`;
  return (
    <div>
      Cannot proceed with flash of SECONDARY raw data to PRIMARY, because the
      PRIMARY and/or SECONDARY ingest buckets are not empty. Below are the file
      tags present in the ingest buckets.
      <br />
      <h3>
        PRIMARY INGEST BUCKET: (<a href={primaryBucketURL}>link</a>)
      </h3>
      {currentRawFileProcessingStatus.unprocessedFilesInPrimary.length === 0 ? (
        <p>EMPTY</p>
      ) : (
        <ul>
          {currentRawFileProcessingStatus.unprocessedFilesInPrimary.map((o) => (
            <li>
              {o.fileTag}: {o.numberFilesInBucket}
            </li>
          ))}
        </ul>
      )}
      <h3>
        SECONDARY INGEST BUCKET (<a href={secondaryBucketURL}>link</a>)
      </h3>
      {currentRawFileProcessingStatus.unprocessedFilesInSecondary.length ===
      0 ? (
        <p>EMPTY</p>
      ) : (
        <ul>
          {currentRawFileProcessingStatus.unprocessedFilesInSecondary.map(
            (o) => (
              <li>{o.fileTag}</li>
            )
          )}
        </ul>
      )}
      <h3 style={{ color: "green" }}>
        Regardless of ingest bucket status, you may proceed with cleaning up the
        secondary instance and canceling the rerun in SECONDARY:{" "}
        <Button type="primary" onClick={onSelectProceed}>
          CLEAN UP SECONDARY + CANCEL RAW DATA REIMPORT
        </Button>
      </h3>
    </div>
  );
};

export interface LegacyStyledStepContentProps {
  // Text to be displayed for this step
  description: JSX.Element;

  // Title of button that actually performs an action. If not present,
  // only a 'Mark done' button will be present for a given step.
  actionButtonTitle?: string;
  // Action that will be performed when the action button is clicked.
  onActionButtonClick?: () => Promise<Response>;

  // Section to move to if this step succeeds.
  nextSection?: number;

  // Whether the action button on a step should be enabled. The Mark Done button
  // is always enabled.
  actionButtonEnabled?: boolean;
}

const LegacyStyledStepContent = ({
  actionButtonTitle,
  onActionButtonClick,
  nextSection,
  description,
  actionButtonEnabled,
}: LegacyStyledStepContentProps): JSX.Element => {
  const [loading, setLoading] = React.useState(false);
  const {
    fetchLegacyInstanceStatusData: setStatusData,
    incrementCurrentStep,
    moveToNextChecklistSection,
  } = useLegacyFlashChecklistStore();

  return (
    <>
      {description}
      {onActionButtonClick && (
        <Button
          type="primary"
          disabled={!actionButtonEnabled}
          onClick={async () => {
            setLoading(true);
            const succeeded = await runAndCheckStatus(onActionButtonClick);
            if (succeeded) {
              await setStatusData();
              await incrementCurrentStep();
              if (nextSection !== undefined) {
                await moveToNextChecklistSection(nextSection);
              }
            }
            setLoading(false);
          }}
          loading={loading}
          style={{ marginRight: 5 }}
        >
          {actionButtonTitle}
        </Button>
      )}
      <Button
        type={onActionButtonClick ? undefined : "primary"}
        disabled={false}
        onClick={async () => {
          setLoading(true);
          await setStatusData();
          await incrementCurrentStep();
          setLoading(false);
          if (nextSection !== undefined) {
            await moveToNextChecklistSection(nextSection);
          }
        }}
        loading={loading}
      >
        Mark Done
      </Button>
    </>
  );
};

export default observer(LegacyStyledStepContent);
