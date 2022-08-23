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
import React from "react";

import { useStore } from "../../stores";
import { removeSnakeCase } from "../../utils";
import { Badge, BadgeColorMapping } from "../Badge";
import { Loading } from "../Loading";
import { showToast } from "../Toast";
import {
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

export const UploadedFiles: React.FC<{
  isLoading: boolean;
  fetchError: boolean;
  uploadedFiles: (UploadedFile | UploadedFileAttempt)[];
}> = observer(({ isLoading, fetchError, uploadedFiles }) => {
  const dataUploadColumnTitles = [
    "Filename",
    "Date Uploaded",
    "Date Ingested",
    "System",
    "Uploaded By",
  ];
  const { reportStore } = useStore();

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

  const translateBackendFileStatus = (status: UploadedFileStatus): string => {
    if (status === "UPLOADED") return "PENDING";
    if (status === "INGESTED") return "PROCESSED";
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
          : "GREY",
        badgeText: fileStatus?.toLowerCase() || "Uploading...",
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
      badgeColor: file.status ? uploadStatusColorMapping[file.status] : "GREY",
      badgeText: file.status?.toLowerCase() || "Uploading...",
      dateUploaded: "--",
      dateIngested: "--",
      uploadedBy: "--",
    };
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
          const {
            key,
            id,
            selected,
            name,
            badgeColor,
            badgeText,
            dateUploaded,
            dateIngested,
            system,
            uploadedBy,
          } = getFileRowDetails(fileDetails);

          return (
            <ExtendedRow
              key={key}
              selected={selected}
              onClick={() => id && handleDownload(id, name)}
            >
              {/* Filename */}
              <ExtendedCell>
                <span>{name}</span>
                <Badge color={badgeColor}>{badgeText}</Badge>
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

              {/* Uploaded By */}
              <ExtendedCell>{uploadedBy}</ExtendedCell>
            </ExtendedRow>
          );
        })}
      </UploadedFilesTable>
    </UploadedFilesContainer>
  );
});
