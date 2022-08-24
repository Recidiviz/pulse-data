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

import { Permission } from "../../shared/types";
import { useStore } from "../../stores";
import { removeSnakeCase } from "../../utils";
import downloadIcon from "../assets/download-icon.png";
import { Badge, BadgeColorMapping, BadgeColors } from "../Badge";
import { Loading } from "../Loading";
import { showToast } from "../Toast";
import {
  ActionButton,
  ActionsContainer,
  DownloadIcon,
  ExtendedCell,
  ExtendedLabelCell,
  ExtendedLabelRow,
  ExtendedRow,
  ModalErrorWrapper,
  ModalLoadingWrapper,
  UploadedFile,
  UploadedFileAttempt,
  UploadedFilesContainer,
  UploadedFilesTable,
  UploadedFileStatus,
} from ".";

export const UploadedFileRow: React.FC<{
  fileRowDetails: {
    key: string;
    id?: number;
    selected: boolean;
    name: string;
    badgeColor: BadgeColors;
    badgeText: string;
    dateUploaded: string;
    dateIngested: string;
    system?: string;
    uploadedBy: string;
  };
  deleteUploadedFile: (spreadsheetID: number) => void;
  updateUploadedFileStatus: (
    spreadsheetID: number,
    status: UploadedFileStatus
  ) => Promise<void>;
}> = observer(
  ({ fileRowDetails, deleteUploadedFile, updateUploadedFileStatus }) => {
    const { reportStore, userStore } = useStore();
    const [isDownloading, setIsDownloading] = useState(false);
    const [rowHovered, setRowHovered] = useState(false);

    const handleDownload = async (spreadsheetID: number, name: string) => {
      setIsDownloading(true);

      const response = await reportStore.fetchSpreadsheetBlob(spreadsheetID);

      if (response instanceof Error) {
        setIsDownloading(false);
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
        setIsDownloading(false);
      }
    };

    const {
      id,
      selected,
      name,
      badgeColor,
      badgeText,
      dateUploaded,
      dateIngested,
      system,
      uploadedBy,
    } = fileRowDetails;

    return (
      <ExtendedRow
        selected={selected}
        onClick={() => id && handleDownload(id, name)}
        onMouseOver={() => setRowHovered(true)}
        onMouseLeave={() => setRowHovered(false)}
      >
        {/* Filename */}
        <ExtendedCell>
          {rowHovered && id && <DownloadIcon src={downloadIcon} alt="" />}
          <span>{name}</span>
          <Badge
            color={badgeColor}
            loading={isDownloading || badgeText === "Uploading"}
          >
            {isDownloading ? "Downloading" : badgeText}
          </Badge>
        </ExtendedCell>

        {/* Date Uploaded */}
        <ExtendedCell capitalize>
          <span>{dateUploaded}</span>
        </ExtendedCell>

        {/* Date Ingested */}
        <ExtendedCell>
          <span>{dateIngested}</span>
        </ExtendedCell>

        {/* System */}
        <ExtendedCell capitalize>
          <span>{system}</span>
        </ExtendedCell>

        {rowHovered &&
          id &&
          userStore.permissions.includes(Permission.RECIDIVIZ_ADMIN) && (
            <ActionsContainer onClick={(e) => e.stopPropagation()}>
              {(badgeText === "processed" || badgeText === "error") && (
                <ActionButton
                  onClick={() => updateUploadedFileStatus(id, "UPLOADED")}
                >
                  Mark as Pending
                </ActionButton>
              )}
              {(badgeText === "pending" || badgeText === "error") && (
                <ActionButton
                  onClick={() => updateUploadedFileStatus(id, "INGESTED")}
                >
                  Mark as Processed
                </ActionButton>
              )}
              {badgeText !== "error" && (
                <ActionButton
                  onClick={() => updateUploadedFileStatus(id, "ERRORED")}
                >
                  Mark as Error
                </ActionButton>
              )}
              <ActionButton red onClick={() => deleteUploadedFile(id)}>
                Delete
              </ActionButton>
            </ActionsContainer>
          )}
        {/* Uploaded By */}
        <ExtendedCell>{uploadedBy}</ExtendedCell>
      </ExtendedRow>
    );
  }
);

export const UploadedFiles: React.FC<{
  isLoading: boolean;
  fetchError: boolean;
  uploadedFiles: (UploadedFile | UploadedFileAttempt)[];
  setUploadedFiles: React.Dispatch<
    React.SetStateAction<(UploadedFileAttempt | UploadedFile)[]>
  >;
}> = observer(({ isLoading, fetchError, uploadedFiles, setUploadedFiles }) => {
  const { reportStore } = useStore();
  const dataUploadColumnTitles = [
    "Filename",
    "Date Uploaded",
    "Date Ingested",
    "System",
    "Uploaded By",
  ];

  const isUploadedFile = (
    file: UploadedFile | UploadedFileAttempt
  ): file is UploadedFile => {
    return (file as UploadedFile).id !== undefined;
  };

  const uploadStatusColorMapping: BadgeColorMapping = {
    UPLOADED: "ORANGE",
    INGESTED: "GREEN",
    ERRORED: "RED",
  };

  const translateBackendFileStatus = (status: UploadedFileStatus): string => {
    if (status === "UPLOADED") return "PENDING";
    if (status === "INGESTED") return "PROCESSED";
    if (status === "ERRORED") return "ERROR";
    return status;
  };

  const getFileRowDetails = (file: UploadedFile | UploadedFileAttempt) => {
    const fileStatus = file.status && translateBackendFileStatus(file.status);

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
          : "ORANGE",
        badgeText: fileStatus?.toLowerCase() || "Uploading",
        dateUploaded: formatDate(file.uploaded_at),
        dateIngested: file.ingested_at ? formatDate(file.ingested_at) : "--",
        system: removeSnakeCase(file.system).toLowerCase(),
        uploadedBy: file.uploaded_by,
      };
    }
    return {
      key: `${file.name}-${file.upload_attempt_timestamp}`,
      selected: false,
      name: file.name,
      badgeColor: file.status
        ? uploadStatusColorMapping[file.status]
        : "ORANGE",
      badgeText: fileStatus?.toLowerCase() || "Uploading",
      dateUploaded: "--",
      dateIngested: "--",
      uploadedBy: "--",
    };
  };

  const deleteUploadedFile = async (spreadsheetID: number) => {
    const response = await reportStore.deleteUploadedSpreadsheet(spreadsheetID);

    if (response instanceof Error) {
      return showToast(response.message, false, "red");
    }

    return setUploadedFiles((prev) => {
      const filteredFiles = prev.filter(
        (file) => (file as UploadedFile).id !== spreadsheetID
      );

      return filteredFiles;
    });
  };

  const updateUploadedFileStatus = async (
    spreadsheetID: number,
    status: UploadedFileStatus
  ) => {
    const response = await reportStore.updateFileStatus(spreadsheetID, status);

    if (response instanceof Error) {
      return showToast(response.message, false, "red");
    }

    return setUploadedFiles((prev) => {
      const updatedFilesList = prev.map((file) => {
        if (isUploadedFile(file) && file.id === spreadsheetID) {
          return { ...file, status };
        }
        return file;
      });

      return updatedFilesList;
    });
  };

  if (isLoading) {
    return (
      <ModalLoadingWrapper>
        <Loading />
      </ModalLoadingWrapper>
    );
  }

  if (fetchError) {
    return (
      <ModalErrorWrapper>
        Failed to retrieve uploaded files. Please refresh and try again.
      </ModalErrorWrapper>
    );
  }

  return (
    <UploadedFilesContainer>
      <UploadedFilesTable>
        <ExtendedLabelRow>
          {dataUploadColumnTitles.map((title) => (
            <ExtendedLabelCell key={title}>{title}</ExtendedLabelCell>
          ))}
        </ExtendedLabelRow>

        {uploadedFiles.map((fileDetails) => {
          const fileRowDetails = getFileRowDetails(fileDetails);

          return (
            <UploadedFileRow
              key={fileRowDetails.key}
              fileRowDetails={fileRowDetails}
              deleteUploadedFile={deleteUploadedFile}
              updateUploadedFileStatus={updateUploadedFileStatus}
            />
          );
        })}
      </UploadedFilesTable>
    </UploadedFilesContainer>
  );
});
