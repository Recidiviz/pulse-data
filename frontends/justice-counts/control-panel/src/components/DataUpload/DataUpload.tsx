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

import { Dropdown, DropdownMenu } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React, { Fragment, useEffect, useState } from "react";

import { useStore } from "../../stores";
import { removeSnakeCase } from "../../utils";
import SpreadsheetIcon from "../assets/spreadsheet-icon.png";
import UploadIcon from "../assets/upload-icon.png";
import { ExtendedDropdownMenuItem, ExtendedDropdownToggle } from "../Menu";
import { PageHeader, TabbedItem, TabbedOptions } from "../Reports";
import { showToast } from "../Toast";
import {
  Button,
  ButtonWrapper,
  DropdownItemUploadInput,
  ExtendedTabbedBar,
  Icon,
  Instructions,
  MediumPageTitle,
  ModalBody,
  UploadButtonInput,
  UploadButtonLabel,
  UploadedFiles,
} from ".";
import {
  GeneralInstructions,
  SystemsInstructions,
} from "./InstructionsTemplate";

export type UploadedFileStatus = "UPLOADED" | "INGESTED" | "ERRORED";

export type UploadedFileAttempt = {
  name: string;
  upload_attempt_timestamp: number;
  status?: "ERRORED";
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

/**
 * The systems in EXCLUDED_SYSTEMS are sub-systems of the SUPERVISION system,
 * and should not render a separate template & instructions.
 *
 * Example: if an agency has the following systems ["SUPERVISION", "PAROLE", "PROBATION"],
 * the UI should render a template & instructions for the SUPERVISION system.
 */
export const EXCLUDED_SYSTEMS = ["PAROLE", "PROBATION", "POST_RELEASE"];

export const DataUpload: React.FC = observer(() => {
  const { userStore, reportStore } = useStore();
  const dataUploadMenuItems = ["Instructions", "Uploaded Files"];
  const acceptableFileTypes = [
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
  ];
  const filteredUserSystems =
    userStore.currentAgency?.systems.filter(
      (system) => !EXCLUDED_SYSTEMS.includes(system)
    ) || [];
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

  const [isLoading, setIsLoading] = useState(true);
  const [fetchError, setFetchError] = useState(false);
  const [systemForUpload, setSystemForUpload] = useState(
    filteredUserSystems[0]
  );
  const [activeMenuItem, setActiveMenuItem] = useState(dataUploadMenuItems[0]);
  const [uploadedFiles, setUploadedFiles] = useState<
    (UploadedFile | UploadedFileAttempt)[]
  >([]);

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
      formData.append("system", systemForUpload);
      formData.append("agency_id", userStore.currentAgencyId.toString());
      const newFileDetails = {
        name: fileName,
        upload_attempt_timestamp: Date.now(),
      };
      if (!acceptableFileTypes.includes(file.type)) {
        showToast(
          "Invalid file type. Please only upload Excel files.",
          false,
          "red",
          3000
        );
        return updateUploadedFilesList({
          ...newFileDetails,
          status: "ERRORED",
        });
      }

      updateUploadedFilesList(newFileDetails);

      const response = await reportStore.uploadExcelSpreadsheet(formData);

      if (response instanceof Error) {
        showToast("Failed to upload. Please try again.", false, "red");
        return updateUploadedFilesList({
          ...newFileDetails,
          status: "ERRORED",
        });
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

  useEffect(() => {
    const fetchFilesList = async () => {
      const response = (await reportStore.getUploadedFilesList()) as
        | Response
        | Error;

      setIsLoading(false);

      if (response instanceof Error) {
        return setFetchError(true);
      }

      setFetchError(false);

      const listOfFiles = (await response.json()) as UploadedFile[];
      setUploadedFiles(listOfFiles);
    };

    fetchFilesList();
  }, [reportStore]);

  return (
    <>
      <PageHeader>
        <MediumPageTitle>Data Upload</MediumPageTitle>

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
            {filteredUserSystems?.length <= 1 ? (
              <UploadButtonLabel htmlFor="upload-data">
                <Button type="blue">
                  <UploadButtonInput
                    type="file"
                    id="upload-data"
                    name="upload-data"
                    accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel"
                    onChange={handleFileUpload}
                    onClick={(e) => {
                      /** reset event state to allow user to re-upload same file (re-trigger the onChange event) */
                      e.currentTarget.value = "";
                      setActiveMenuItem("Uploaded Files");
                    }}
                  />
                  Upload Data
                  <Icon alt="" src={UploadIcon} />
                </Button>
              </UploadButtonLabel>
            ) : (
              <Dropdown>
                <ExtendedDropdownToggle
                  kind="borderless"
                  style={{ marginBottom: 0 }}
                >
                  <Button
                    type="blue"
                    onClick={() => setActiveMenuItem("Uploaded Files")}
                  >
                    Upload Data
                    <Icon alt="" src={UploadIcon} />
                  </Button>
                </ExtendedDropdownToggle>

                <DropdownMenu alignment="right">
                  {filteredUserSystems?.map((system) => (
                    <ExtendedDropdownMenuItem
                      key={system}
                      onClick={() => setSystemForUpload(system)}
                      noPadding
                    >
                      <UploadButtonLabel htmlFor="upload-data">
                        <DropdownItemUploadInput
                          type="file"
                          id="upload-data"
                          name="upload-data"
                          accept="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/vnd.ms-excel"
                          onChange={handleFileUpload}
                          onClick={(e) => {
                            /** reset event state to allow user to re-upload same file (re-trigger the onChange event) */
                            e.currentTarget.value = "";
                          }}
                        />
                        {system.toLowerCase()}
                        <Icon alt="" src={UploadIcon} />
                      </UploadButtonLabel>
                    </ExtendedDropdownMenuItem>
                  ))}
                </DropdownMenu>
              </Dropdown>
            )}
          </ButtonWrapper>
        </ExtendedTabbedBar>
      </PageHeader>

      <ModalBody hasLabelRow={activeMenuItem === "Uploaded Files"}>
        {/* Instructions */}
        {activeMenuItem === "Instructions" && (
          <Instructions>
            <h1>How to Upload Data to Justice Counts</h1>

            {/* Download Templates */}
            <ButtonWrapper>
              {filteredUserSystems?.map((system) => {
                const systemName = removeSnakeCase(system).toLowerCase();
                const systemFileName =
                  systemToTemplateSpreadsheetFileName[system];
                return (
                  <a
                    key={system}
                    href={`./assets/${systemFileName}`}
                    download={systemFileName}
                  >
                    <Button>
                      Download {systemName} Template{" "}
                      <Icon alt="" src={SpreadsheetIcon} grayscale />
                    </Button>
                  </a>
                );
              })}
            </ButtonWrapper>

            {/* General Instructions */}
            <GeneralInstructions systems={filteredUserSystems ?? []} />

            {/* System Specific Instructions */}
            {filteredUserSystems?.map((system) => {
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
        )}

        {/* Uploaded Files */}

        {activeMenuItem === "Uploaded Files" && (
          <UploadedFiles
            isLoading={isLoading}
            uploadedFiles={uploadedFiles}
            fetchError={fetchError}
            setUploadedFiles={setUploadedFiles}
          />
        )}
      </ModalBody>
    </>
  );
});
