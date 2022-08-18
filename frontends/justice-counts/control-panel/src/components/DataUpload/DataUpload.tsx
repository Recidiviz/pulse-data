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

import { observer } from "mobx-react-lite";
import React, { Fragment, useEffect, useState } from "react";

import { useStore } from "../../stores";
import { removeSnakeCase } from "../../utils";
import Logo from "../assets/jc-logo-vector.png";
import SpreadsheetIcon from "../assets/spreadsheet-icon.png";
import UploadIcon from "../assets/upload-icon.png";
import { Badge, BadgeColorMapping } from "../Badge";
import {
  Cell,
  Label,
  PageHeader,
  PageTitle,
  TabbedItem,
  TabbedOptions,
} from "../Reports";
import { showToast } from "../Toast";
import {
  Button,
  ButtonWrapper,
  ExtendedLabelRow,
  ExtendedRow,
  ExtendedTabbedBar,
  Icon,
  Instructions,
  InstructionsContainer,
  InstructionsGraphic,
  InstructionsGraphicWrapper,
  ModalBody,
  UploadButtonInput,
  UploadButtonLabel,
  UploadedFilesContainer,
  UploadedFilesTable,
} from ".";
import {
  GeneralInstructions,
  systemToInstructionsTemplate,
} from "./InstructionsTemplate";

export type UploadedFileStatus = "UPLOADED" | "INGESTED" | "ERROR";

export type UploadedFileAttempt = {
  name: string;
  upload_attempt_timestamp: number;
  status?: "ERROR";
};

export type UploadedFile = {
  name: string;
  id: number;
  uploaded_at: number;
  ingested_at: number;
  uploaded_by: string;
  system: string;
  status: UploadedFileStatus | null;
};

const isUploadedFile = (
  file: UploadedFile | UploadedFileAttempt
): file is UploadedFile => {
  return (file as UploadedFile).id !== undefined;
};

const uploadStatusColorMapping: BadgeColorMapping = {
  UPLOADED: "ORANGE",
  INGESTED: "GREEN",
  ERROR: "RED",
};

export const DataUpload: React.FC = observer(() => {
  const dataUploadMenuItems = ["Instructions", "Uploaded Files"];
  const dataUploadColumnTitles = [
    "Filename",
    "Date Uploaded",
    "Date Ingested",
    "Uploaded By",
  ];
  const [activeMenuItem, setActiveMenuItem] = useState(dataUploadMenuItems[0]);
  const [uploadedFiles, setUploadedFiles] = useState<
    (UploadedFile | UploadedFileAttempt)[]
  >([]);
  const { userStore, reportStore } = useStore();

  const userSystems = userStore.currentAgency?.systems;
  const systemToTemplateSpreadsheetFileName: { [system: string]: string } = {
    LAW_ENFORCEMENT: "LAW_ENFORCEMENT.xlsx",
    PROSECUTION: "PROSECUTION.xlsx",
    DEFENSE: "DEFENSE.xlsx",
    COURTS_AND_PRETRIAL: "COURTS_AND_PRETRIAL.xlsx",
    JAILS: "JAILS.xlsx",
    PRISONS: "PRISONS.xlsx",
    SUPERVISION: "SUPERVISION.xlsx",
    PAROLE: "SUPERVISION.xlsx",
    PROBATION: "SUPERVISION.xlsx",
  };

  const updateUploadedFilesList = (
    fileDetails: UploadedFile | UploadedFileAttempt
  ) => {
    setUploadedFiles((prev) => {
      let matchFound = false;
      const updatedFilesList = prev.map((file) => {
        if (file.name === fileDetails.name && !file.status) {
          matchFound = true;
          return fileDetails;
        }
        return file;
      });

      return matchFound ? updatedFilesList : [fileDetails, ...prev];
    });
  };

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const fileName = file?.name;
    const formData = new FormData();

    if (fileName && userStore.currentAgencyId) {
      formData.append("file", file);
      formData.append("name", fileName);
      formData.append("agency_id", userStore.currentAgencyId.toString());
      const newFileDetails = {
        name: fileName,
        upload_attempt_timestamp: Date.now(),
      };

      updateUploadedFilesList(newFileDetails);

      const response = await reportStore.uploadExcelSpreadsheet(formData);

      if (response instanceof Error) {
        showToast("Failed to upload. Please try again.", false, "red");
        return updateUploadedFilesList({ ...newFileDetails, status: "ERROR" });
      }

      const fullFileDetails = await response?.json();

      showToast(
        "File uploaded successfully and is pending processing by a Justice Counts administrator.",
        true,
        undefined,
        3500
      );
      updateUploadedFilesList(fullFileDetails);
    }
  };

  const getFileRowDetails = (file: UploadedFile | UploadedFileAttempt) => {
    const fileStatus = file.status === "UPLOADED" ? "PENDING" : file.status;

    if (isUploadedFile(file)) {
      const formatDate = (timestamp: number) =>
        Intl.DateTimeFormat("en-US", {
          day: "numeric",
          month: "long",
          year: "numeric",
        }).format(timestamp);

      return {
        key: `${file.name}-${file.id}`,
        id: file.id,
        selected: !file.status,
        name: file.name,
        badgeColor: file.status
          ? uploadStatusColorMapping[file.status]
          : "GREY",
        badgeText: fileStatus?.toLowerCase() || "Uploading...",
        dateUploaded: formatDate(file.uploaded_at),
        dateIngested: file.ingested_at ? formatDate(file.ingested_at) : "--",
        uploadedBy: file.uploaded_by,
      };
    }
    return {
      key: `${file.name}-${file.upload_attempt_timestamp}`,
      selected: false,
      name: file.name,
      badgeColor: file.status ? uploadStatusColorMapping[file.status] : "GREY",
      badgeText: file.status?.toLowerCase() || "Uploading...",
      dateUploaded: "--",
      dateIngested: "--",
      uploadedBy: "--",
    };
  };

  const handleDownload = async (spreadsheetID: number, name: string) => {
    const response = await reportStore.fetchSpreadsheetBlob(spreadsheetID);

    if (response instanceof Error) {
      return showToast("Failed to download. Please try again.", false, "red");
    }

    const data = await response?.blob();

    if (data) {
      const blob = new Blob([data], {
        type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;",
      });

      const link = document.createElement("a");
      const url = window.URL.createObjectURL(blob);
      link.href = url;
      link.download = name;
      link.click();

      window.URL.revokeObjectURL(url);
      link.remove();
    }
  };

  useEffect(() => {
    const fetchFilesList = async () => {
      const response = (await reportStore.getUploadedFilesList()) as
        | Response
        | Error;

      if (response instanceof Error) {
        return showToast("Failed to get files.", false, "red");
      }

      const listOfFiles = (await response.json()) as UploadedFile[];
      setUploadedFiles(listOfFiles);
    };

    fetchFilesList();
  }, [reportStore]);

  return (
    <>
      <PageHeader>
        <PageTitle>Data Upload</PageTitle>

        {/* Data Upload Menu */}
        <ExtendedTabbedBar>
          <TabbedOptions>
            {dataUploadMenuItems.map((item) => (
              <TabbedItem
                key={item}
                id={item}
                selected={item === activeMenuItem}
                onClick={() => setActiveMenuItem(item)}
              >
                {item}
              </TabbedItem>
            ))}
          </TabbedOptions>

          <ButtonWrapper>
            <UploadButtonLabel htmlFor="upload-data">
              <Button type="blue">
                <UploadButtonInput
                  type="file"
                  id="upload-data"
                  name="upload-data"
                  accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel"
                  onChange={handleFileUpload}
                  onClick={() => setActiveMenuItem("Uploaded Files")}
                />
                Upload Data
                <Icon alt="" src={UploadIcon} />
              </Button>
            </UploadButtonLabel>
          </ButtonWrapper>
        </ExtendedTabbedBar>
      </PageHeader>

      <ModalBody hasLabelRow={activeMenuItem === "Uploaded Files"}>
        {/* Instructions */}
        {activeMenuItem === "Instructions" && (
          <InstructionsContainer>
            <InstructionsGraphicWrapper>
              <InstructionsGraphic alt="" src={Logo} />
            </InstructionsGraphicWrapper>
            <Instructions>
              <h1>How to Upload Data to Justice Counts</h1>

              {/* Download Templates */}
              <ButtonWrapper>
                {userSystems?.map((system) => {
                  const systemName = removeSnakeCase(system).toLowerCase();
                  const systemFileName =
                    systemToTemplateSpreadsheetFileName[system];
                  return (
                    <Button key={system}>
                      <a
                        href={`./assets/${systemFileName}`}
                        download={systemFileName}
                      >
                        Download {systemName} Template{" "}
                        <Icon alt="" src={SpreadsheetIcon} grayscale />
                      </a>
                    </Button>
                  );
                })}
              </ButtonWrapper>

              {/* General Instructions */}
              <GeneralInstructions />

              {/* System Specific Instructions */}
              {userSystems?.map((system) => {
                const systemName = removeSnakeCase(system).toLowerCase();
                const systemTemplate = systemToInstructionsTemplate[system];

                return (
                  <Fragment key={systemName}>
                    <h2>{systemName}</h2>
                    {systemTemplate}
                  </Fragment>
                );
              })}
            </Instructions>
          </InstructionsContainer>
        )}

        {/* Uploaded Files */}

        {activeMenuItem === "Uploaded Files" && (
          <UploadedFilesContainer>
            <UploadedFilesTable>
              <ExtendedLabelRow>
                {dataUploadColumnTitles.map((title) => (
                  <Label key={title}>{title}</Label>
                ))}
              </ExtendedLabelRow>

              {uploadedFiles.map((fileDetails) => {
                const {
                  key,
                  id,
                  selected,
                  name,
                  badgeColor,
                  badgeText,
                  dateUploaded,
                  dateIngested,
                  uploadedBy,
                } = getFileRowDetails(fileDetails);

                return (
                  <ExtendedRow
                    key={key}
                    selected={selected}
                    onClick={() => id && handleDownload(id, name)}
                  >
                    {/* Filename */}
                    <Cell>
                      {name}
                      <Badge color={badgeColor}>{badgeText}</Badge>
                    </Cell>

                    {/* Date Uploaded */}
                    <Cell capitalize>{dateUploaded}</Cell>

                    {/* Date Ingested */}
                    <Cell>{dateIngested}</Cell>

                    {/* Uploaded By */}
                    <Cell>{uploadedBy}</Cell>
                  </ExtendedRow>
                );
              })}
            </UploadedFilesTable>
          </UploadedFilesContainer>
        )}
      </ModalBody>
    </>
  );
});
