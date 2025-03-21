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
import { CheckSquareTwoTone, CloseSquareTwoTone } from "@ant-design/icons";
import { Alert, Button, Card, Divider, List, StepProps, Steps } from "antd";
import { observer } from "mobx-react-lite";
import * as React from "react";

import { useFlashChecklistStore } from "./FlashChecklistContext";
import { runAndCheckStatus } from "./FlashUtils";

export interface ContentStepProps extends StepProps {
  content: JSX.Element;
}

export interface CodeBlockProps {
  children: React.ReactNode;
  enabled: boolean;
}

export const CodeBlock = ({
  children,
  enabled,
}: CodeBlockProps): JSX.Element => (
  <code
    style={{
      display: "block",
      whiteSpace: "pre-wrap",
      padding: 10,
      borderRadius: 10,
      backgroundColor: enabled ? "#fafafa" : "#d9d9d9",
      color: enabled ? "rgba(0, 0, 0, 0.85)" : "rgba(0, 0, 0, 0.45)",
    }}
  >
    {children}
  </code>
);

export interface ChecklistSectionHeaderProps {
  children: React.ReactNode;
  currentStepSection: number;
  stepSection: number;
}

export const ChecklistSectionHeader = ({
  children,
  currentStepSection,
  stepSection,
}: ChecklistSectionHeaderProps): JSX.Element => (
  <h1>
    <b style={{ display: "inline-flex" }}>
      {currentStepSection > stepSection ? "COMPLETED-" : ""}
      {children}
    </b>
  </h1>
);

export interface ChecklistSectionProps {
  items: ContentStepProps[];
  headerContents: string;
  currentStep: number;
  currentStepSection: number;
  stepSection: number;
}

export const ChecklistSection = ({
  items,
  headerContents,
  currentStep,
  currentStepSection,
  stepSection,
}: ChecklistSectionProps): JSX.Element => {
  const currentStepsSection =
    currentStepSection === stepSection ? currentStep : 0;
  const actionItemsContent = (
    <div style={{ display: "flex" }}>
      <Card style={{ float: "left", width: "30%" }}>
        <Steps
          current={currentStepsSection}
          direction="vertical"
          size="small"
          items={items.map((item: ContentStepProps) => ({
            ...item,
            title: <div style={{ textWrap: "wrap" }}>{item.title}</div>,
          }))}
        />
      </Card>
      <Card style={{ width: "100%", marginLeft: "1%" }}>
        {items.length > 0 ? items[currentStepsSection]?.content : undefined}
      </Card>
    </div>
  );

  const checklistHeaderContents =
    currentStepSection > stepSection
      ? `COMPLETED -${headerContents}`
      : headerContents;

  return (
    <div
      style={{
        opacity: currentStepSection === stepSection ? 1 : 0.25,
        pointerEvents: currentStepSection === stepSection ? "initial" : "none",
      }}
    >
      <Divider orientation="left">{checklistHeaderContents}</Divider>
      {items.length !== 0 ? actionItemsContent : undefined}
    </div>
  );
};

export const FlashDecisionCriteria = (): JSX.Element => {
  const flashStore = useFlashChecklistStore();
  if (!flashStore.stateInfo) throw new Error("Should have state code set");
  const formattedStateCode = flashStore.stateInfo.code
    .toLowerCase()
    .replaceAll("_", "-");

  const flashCriteria = [
    {
      criteria: "All ingest buckets must be empty",
      status: flashStore.currentRawFileProcessingStatus.areIngestBucketsEmpty(),
      description:
        flashStore.currentRawFileProcessingStatus.ingestBucketSummary(
          flashStore.projectId,
          formattedStateCode
        ),
    },
    {
      criteria: "There are processed files in secondary",
      status:
        flashStore.currentRawFileProcessingStatus.hasProcessedFilesInSecondary(),
      description:
        "We only want to flash to primary if we have valid data in secondary",
    },
    {
      criteria: "There are NOT unprocessed files in secondary",
      status:
        !flashStore.currentRawFileProcessingStatus.hasUnprocessedFilesInSecondary(),
      description:
        flashStore.currentRawFileProcessingStatus.unprocessedSecondaryDescription(),
    },
    {
      criteria: " Secondary is not stale",
      status: !flashStore.currentStaleSecondaryStatus.isStale(),
      description: flashStore.currentStaleSecondaryStatus.statusDescription(),
    },
  ];

  return (
    <List
      itemLayout="vertical"
      dataSource={flashCriteria}
      renderItem={(item) => (
        <List.Item.Meta
          title={item.criteria}
          description={item.description}
          avatar={<ConditionalColoredSquare boolVal={item.status} />}
        />
      )}
    />
  );
};

export const ConditionalColoredSquare = ({
  boolVal,
}: {
  boolVal: boolean;
}): JSX.Element => {
  return boolVal ? (
    <CheckSquareTwoTone twoToneColor="green" />
  ) : (
    <CloseSquareTwoTone twoToneColor="red" />
  );
};

export const CannotFlashDecisionComponent = ({
  onSelectProceed,
}: {
  onSelectProceed: () => void;
}): JSX.Element => {
  return (
    <div>
      <Alert
        message="Cannot proceed with flash to primary. All of the following conditions must be met:"
        description={<FlashDecisionCriteria />}
        type="error"
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

export const FlashReadyDecisionComponent = ({
  onSelectProceed,
  onSelectCancel,
}: {
  onSelectProceed: () => void;
  onSelectCancel: () => void;
}): JSX.Element => {
  return (
    <div>
      <Alert
        message="The SECONDARY results are ready to be flashed to PRIMARY"
        description={<FlashDecisionCriteria />}
        type="success"
        showIcon
      />
      <br />
      <h3>
        Now that SECONDARY results are ready to be flashed to PRIMARY, would you
        like to:
      </h3>
      <ul>
        <li>
          <b>Proceed with flash to PRIMARY.</b> Results in SECONDARY have been
          validated and can be copied to PRIMARY.
        </li>
        <li>
          <b>Cancel secondary rerun.</b> Delete and clean up results in
          SECONDARY and do not copy over to PRIMARY.
        </li>
      </ul>
      <Button type="primary" onClick={onSelectProceed}>
        Proceed with Flash
      </Button>
      <Button onClick={onSelectCancel}>Cancel Reimport</Button>
    </div>
  );
};

export interface StyledStepContentProps {
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

  // If this is true, we are on the last section and step so we should display a return
  // to decision page button
  returnButton?: boolean;
}

const StyledStepContent = ({
  actionButtonTitle,
  onActionButtonClick,
  nextSection,
  description,
  actionButtonEnabled,
  returnButton,
}: StyledStepContentProps): JSX.Element => {
  const [actionLoading, setActionLoading] = React.useState(false);
  const [markDoneLoading, setMarkDoneLoading] = React.useState(false);
  const {
    fetchIsFlashInProgress,
    fetchResourceLockStatus,
    incrementCurrentStep,
    moveToNextChecklistSection,
    resetChecklist,
  } = useFlashChecklistStore();

  return (
    <>
      {description}
      {!returnButton && onActionButtonClick && (
        <Button
          type="primary"
          disabled={!actionButtonEnabled}
          onClick={async () => {
            setActionLoading(true);
            const succeeded = await runAndCheckStatus(onActionButtonClick);
            if (succeeded) {
              await fetchIsFlashInProgress();
              await fetchResourceLockStatus();
              await incrementCurrentStep();
              if (nextSection !== undefined) {
                await moveToNextChecklistSection(nextSection);
              }
            }
            setActionLoading(false);
          }}
          loading={actionLoading}
          style={{ marginRight: 5 }}
        >
          {actionButtonTitle}
        </Button>
      )}
      {!returnButton && (
        <Button
          type={onActionButtonClick ? undefined : "primary"}
          disabled={false}
          onClick={async () => {
            setMarkDoneLoading(true);
            await fetchIsFlashInProgress();
            await fetchResourceLockStatus();
            await incrementCurrentStep();
            setMarkDoneLoading(false);
            if (nextSection !== undefined) {
              await moveToNextChecklistSection(nextSection);
            }
          }}
          loading={markDoneLoading}
        >
          Mark Done
        </Button>
      )}
      {returnButton && (
        <Button
          type="primary"
          onClick={async () => {
            await resetChecklist();
          }}
        >
          Return to decision page
        </Button>
      )}
    </>
  );
};

export default observer(StyledStepContent);
