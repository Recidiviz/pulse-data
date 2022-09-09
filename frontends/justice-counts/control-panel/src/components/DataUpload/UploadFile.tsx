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

import React, { Fragment, useEffect, useRef, useState } from "react";

import { AgencySystems } from "../../shared/types";
import { removeSnakeCase } from "../../utils";
import { ReactComponent as FileIcon } from "../assets/file-icon.svg";
import { showToast } from "../Toast";
import {
  DragDropContainer,
  GeneralInstructions,
  Instructions,
  SystemsInstructions,
  UploadButtonInput,
  UploadButtonLabel,
  UploadFileContainer,
} from ".";

type UploadFileProps = {
  userSystems: AgencySystems[];
  setIsLoading: React.Dispatch<React.SetStateAction<boolean>>;
  setSelectedFile: React.Dispatch<React.SetStateAction<File | undefined>>;
  handleFileUpload: (file: File, system: AgencySystems) => Promise<void>;
};

export const UploadFile: React.FC<UploadFileProps> = ({
  userSystems,
  setIsLoading,
  setSelectedFile,
  handleFileUpload,
}) => {
  const dragDropAreaRef = useRef<HTMLDivElement>(null);
  const [dragging, setDragging] = useState<boolean>(false);
  const acceptableFileTypes = [
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
  ];

  const handleFileUploadAttempt = (
    e: React.ChangeEvent<HTMLInputElement> | DragEvent
  ) => {
    const files = "dataTransfer" in e ? e.dataTransfer?.files : e.target.files;

    if (!files) return;
    if (!acceptableFileTypes.includes(files[0].type)) {
      return showToast(
        "Invalid file type. Please only upload Excel files.",
        false,
        "red",
        3000
      );
    }

    setIsLoading(true);
    if (userSystems.length > 1) {
      setIsLoading(false);
      setSelectedFile(files[0]);
    } else {
      handleFileUpload(files[0], userSystems[0]);
    }
  };

  useEffect(
    () => {
      const dragDropArea = dragDropAreaRef.current;

      const handleDragOver = (e: DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        if (!dragging) setDragging(true);
      };

      const handleDragLeave = (e: DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDragging(false);
      };

      const handleDrop = (e: DragEvent) => {
        e.preventDefault();
        e.stopPropagation();
        setDragging(false);

        if (e.dataTransfer?.files.length) {
          handleFileUploadAttempt(e);
        }
      };

      if (dragDropArea) {
        dragDropArea.addEventListener("dragover", handleDragOver);
        dragDropArea.addEventListener("dragleave", handleDragLeave);
        dragDropArea.addEventListener("drop", handleDrop);
      }

      return () => {
        if (dragDropArea) {
          dragDropArea.removeEventListener("dragover", handleDragOver);
          dragDropArea.removeEventListener("dragleave", handleDragLeave);
          dragDropArea.removeEventListener("drop", handleDrop);
        }
      };
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    []
  );

  return (
    <UploadFileContainer>
      <Instructions>
        {/* General Instructions */}
        <GeneralInstructions systems={userSystems} />

        {/* System Specific Instructions */}
        {userSystems?.map((system) => {
          const systemName = removeSnakeCase(system).toLowerCase();
          const systemTemplate = <SystemsInstructions system={system} />;

          return (
            <Fragment key={systemName}>
              <h2>{systemName}</h2>
              {systemTemplate}
            </Fragment>
          );
        })}
      </Instructions>

      <DragDropContainer ref={dragDropAreaRef} dragging={dragging}>
        <FileIcon style={{ marginBottom: 15 }} />
        <span>
          Drag & drop a file or{" "}
          <UploadButtonLabel htmlFor="upload-data">
            <UploadButtonInput
              type="file"
              id="upload-data"
              name="upload-data"
              accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel"
              onChange={handleFileUploadAttempt}
              onClick={(e) => {
                /** reset event state to allow user to re-upload same file (re-trigger the onChange event) */
                e.currentTarget.value = "";
              }}
            />
            browse your computer
          </UploadButtonLabel>
        </span>
        <span style={{ opacity: 0.5 }}>Accepted file types: .xls, .xlsx</span>
      </DragDropContainer>
    </UploadFileContainer>
  );
};
