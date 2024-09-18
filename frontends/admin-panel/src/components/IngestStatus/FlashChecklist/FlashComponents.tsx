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
import { Button, Card, StepProps, Steps } from "antd";
import * as React from "react";

import { useNewFlashChecklistStore } from "./FlashChecklistStore";

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
  children?: React.ReactNode;
  items: ContentStepProps[];
  headerContents: React.ReactNode;
  currentStep: number;
  currentStepSection: number;
  stepSection: number;
}

export const ChecklistSection = ({
  children,
  items,
  headerContents,
  currentStep,
  currentStepSection,
  stepSection,
}: ChecklistSectionProps): JSX.Element => {
  const currentStepsSection =
    currentStepSection === stepSection ? currentStep : 0;

  return (
    <div
      style={{
        opacity: currentStepSection === stepSection ? 1 : 0.25,
        pointerEvents: currentStepSection === stepSection ? "initial" : "none",
      }}
    >
      <ChecklistSectionHeader
        currentStepSection={currentStepSection}
        stepSection={stepSection}
      >
        {headerContents}
      </ChecklistSectionHeader>
      <div style={{ display: "flex" }}>
        <Card style={{ float: "left", width: "30%" }}>
          <Steps
            progressDot
            current={currentStepsSection}
            direction="vertical"
            size="small"
            items={items.map((item: ContentStepProps) => ({
              ...item,
              title: <div style={{ textWrap: "wrap" }}>{item.title}</div>,
            }))}
          >
            {children}
          </Steps>
        </Card>
        <Card style={{ width: "100%", marginLeft: "1%" }}>
          {items.length > 0 ? items[currentStepsSection]?.content : undefined}
        </Card>
      </div>
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
      The SECONDARY results are ready to be flashed to PRIMARY. Would you like
      to:
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
      <Button onClick={onSelectCancel}>Cancel Rerun</Button>
    </div>
  );
};

export const CannotFlashDecisionNonEmptyBucketComponent = ({
  onSelectProceed,
}: {
  onSelectProceed: () => void;
}): JSX.Element => {
  const { projectId, stateInfo, currentRawFileProcessingStatus } =
    useNewFlashChecklistStore();

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
