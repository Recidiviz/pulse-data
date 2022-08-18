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
import React, { useEffect, useState } from "react";
import styled from "styled-components/macro";

import { useStore } from "../../stores";
import DownloadIcon from "../assets/download-icon.png";
import Logo from "../assets/jc-logo-vector.png";
import SpreadsheetIcon from "../assets/spreadsheet-icon.png";
import UploadIcon from "../assets/upload-icon.png";
import { Badge, BadgeColorMapping } from "../Badge";
import { palette, typography } from "../GlobalStyles";
import {
  Cell,
  Label,
  LabelRow,
  PageHeader,
  PageTitle,
  Row,
  TabbedBar,
  TabbedItem,
  TabbedOptions,
  Table,
} from "../Reports";
import { showToast } from "../Toast";

const HEADER_HEIGHT = 170;
const ROW_HEIGHT = 42;

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

export type ButtonTypes = "borderless" | "blue";

export const ExtendedTabbedBar = styled(TabbedBar)`
  height: 66px;
`;

export const ExtendedLabelRow = styled(LabelRow)`
  position: fixed;
  top: ${HEADER_HEIGHT}px;
  background: ${palette.solid.white};
  z-index: 1;
`;

export const DataUploadButton = styled.div`
  display: flex;
  justify-content: center;
  align-items: center;
  background: rgba(0, 115, 229, 0.1);
  padding: 10px 15px;
  color: ${palette.solid.blue};

  &:hover {
    cursor: pointer;
    opacity: 0.9;
  }
`;

export const InstructionsContainer = styled.div`
  height: 100%;
  display: flex;
`;

export const InstructionsGraphicWrapper = styled.div`
  width: 37%;
  height: 100%;
  background: linear-gradient(
    217.89deg,
    #0073e5 0%,
    rgba(0, 115, 229, 0.5) 100%
  );
  overflow: hidden;
`;

export const InstructionsGraphic = styled.img`
  width: 891px;
  position: absolute;
  left: -267px;
  top: 372px;
  opacity: 0.2;
`;

export const Instructions = styled.div`
  width: 63%;
  height: 100%;
  padding: 44px 70px;
  display: flex;
  flex-direction: column;
  overflow-y: scroll;
`;

export const UploadedFilesContainer = styled.div`
  height: 100%;
  padding: ${ROW_HEIGHT}px 0;
  overflow-y: scroll;
`;

export const UploadedFilesTable = styled(Table)`
  padding: unset;
`;

export const ExtendedRow = styled(Row)`
  color: ${({ selected }) => selected && palette.highlight.grey9};
`;

export const ModalBody = styled.div<{ hasLabelRow?: boolean }>`
  width: 100%;
  height: calc(100% - ${HEADER_HEIGHT}px);
  position: absolute;
  top: ${HEADER_HEIGHT}px;
`;

export const ButtonWrapper = styled.div`
  display: flex;
  gap: 10px;
  margin: 13px 0;
`;

export const Button = styled.div<{ type?: ButtonTypes }>`
  ${typography.sizeCSS.normal};
  display: flex;
  align-items: center;
  padding: 10px 15px;
  border-radius: 3px;

  ${({ type }) => {
    if (type === "borderless") {
      return `
        background: none;
        color: ${palette.highlight.grey10};
      `;
    }
    if (type === "blue") {
      return `
        background: ${palette.solid.blue};
        color: ${palette.solid.white};
      `;
    }
    return `
      background: ${palette.highlight.grey1};
      color: ${palette.highlight.grey10};
    `;
  }}

  &:hover {
    cursor: pointer;
    ${({ type }) => {
      if (type === "borderless") {
        return `opacity: 0.8;`;
      }
      if (type === "blue") {
        return `opacity: 0.9;`;
      }
      return `background: ${palette.highlight.grey2};`;
    }};
  }
`;

export const UploadButtonLabel = styled.label`
  display: block;
`;

export const UploadButtonInput = styled.input`
  display: none;
`;

export const Icon = styled.img<{ grayscale?: boolean }>`
  width: 16px;
  aspect-ratio: auto;
  margin-left: 10px;
  ${({ grayscale }) => grayscale && `filter: grayscale(1);`}
`;

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
            <Button type="borderless">
              Download Template <Icon alt="" src={SpreadsheetIcon} />
            </Button>
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
              <ButtonWrapper>
                <Button>
                  Download Instructions <Icon alt="" src={DownloadIcon} />
                </Button>
                <Button>
                  Download Template{" "}
                  <Icon alt="" src={SpreadsheetIcon} grayscale />
                </Button>
              </ButtonWrapper>
              <p>
                Agencies participating in Justice Counts have two options for
                reporting their assigned metrics: filling out the autogenerated
                reports on the Reports page, or uploading a spreadsheet.
              </p>
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
