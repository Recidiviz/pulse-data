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
import React, { useState } from "react";
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

const HEADER_HEIGHT = 170;
const ROW_HEIGHT = 42;

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

type ButtonTypes = "borderless" | "blue";

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

type UploadedFile = {
  name: string;
  date_uploaded: string;
  date_ingested: string | null;
  uploaded_by: string | undefined;
  status: string | null;
};

const uploadStatusColorMapping: BadgeColorMapping = {
  Pending: "ORANGE",
  Ingested: "GREEN",
  Error: "RED",
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
  const [uploadedFiles, setUploadedFiles] = useState<UploadedFile[]>([]);
  const { userStore } = useStore();

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    const fileName = file?.name;

    if (fileName) {
      const normalizedDateNow = Intl.DateTimeFormat("en-US", {
        day: "numeric",
        month: "long",
        year: "numeric",
      }).format(Date.now());

      const uploadedFileDetails = {
        name: fileName,
        date_uploaded: normalizedDateNow,
        date_ingested: null,
        uploaded_by: userStore.nameOrEmail,
        status: null,
      };

      setUploadedFiles((prev) => {
        let matchFound = false;
        const updatedFilesList = prev.map((list) => {
          if (list.name === fileName) {
            matchFound = true;
            return uploadedFileDetails;
          }
          return list;
        });

        return matchFound ? updatedFilesList : [uploadedFileDetails, ...prev];
      });

      // Mock successful server response
      setTimeout(() => {
        setUploadedFiles((prev) => {
          return prev.map((list) => {
            if (list.name === fileName) {
              return { ...list, status: "Pending" };
            }
            return list;
          });
        });
      }, 2000);
    }
  };

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
                  accept=".csv, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel"
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
              {uploadedFiles.map((fileDetails) => (
                <ExtendedRow
                  key={fileDetails.name}
                  selected={!fileDetails.status}
                >
                  {/* Filename */}
                  <Cell>
                    {fileDetails.name}
                    <Badge
                      color={
                        fileDetails.status
                          ? uploadStatusColorMapping[fileDetails.status]
                          : "GREY"
                      }
                    >
                      {fileDetails.status || "Uploading..."}
                    </Badge>
                  </Cell>

                  {/* Date Uploaded */}
                  <Cell capitalize>{fileDetails.date_uploaded}</Cell>

                  {/* Date Ingested */}
                  <Cell>{fileDetails.date_ingested || "--"}</Cell>

                  {/* Uploaded By */}
                  <Cell>{fileDetails.uploaded_by}</Cell>
                </ExtendedRow>
              ))}
            </UploadedFilesTable>
          </UploadedFilesContainer>
        )}
      </ModalBody>
    </>
  );
});
