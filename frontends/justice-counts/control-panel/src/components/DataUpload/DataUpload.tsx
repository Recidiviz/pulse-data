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
import React, { Fragment, useState } from "react";
import { useNavigate } from "react-router-dom";

import { AgencySystems } from "../../shared/types";
import { useStore } from "../../stores";
import { removeSnakeCase } from "../../utils";
import { ReactComponent as ErrorIcon } from "../assets/error-icon.svg";
import logoImg from "../assets/jc-logo-vector.png";
import { ReactComponent as WarningIcon } from "../assets/warning-icon.svg";
import { Logo, LogoContainer } from "../Header";
import { Loading } from "../Loading";
import { showToast } from "../Toast";
import {
  Button,
  ButtonWrapper,
  DataUploadContainer,
  DataUploadHeader,
  ErrorAdditionalInfo,
  ErrorIconWrapper,
  ErrorMessageDescription,
  ErrorMessageTitle,
  ErrorMessageWrapper,
  FileName,
  MetricTitle,
  SystemSelection,
  UploadFile,
  UserPromptContainer,
  UserPromptDescription,
  UserPromptError,
  UserPromptErrorContainer,
  UserPromptTitle,
  UserPromptWrapper,
} from ".";

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
  system: AgencySystems;
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

export const systemToTemplateSpreadsheetFileName: { [system: string]: string } =
  {
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

export const DataUpload: React.FC = observer(() => {
  const { userStore, reportStore } = useStore();
  const navigate = useNavigate();
  const userSystems =
    userStore.currentAgency?.systems.filter(
      (system) => !EXCLUDED_SYSTEMS.includes(system)
    ) || [];

  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [uploadError, setUploadError] = useState<boolean>(false);
  const [selectedFile, setSelectedFile] = useState<File>();
  const [selectedSystem, setSelectedSystem] = useState<
    AgencySystems | undefined
  >(userSystems.length === 1 ? userSystems[0] : undefined);

  const handleFileUpload = async (
    file: File,
    system: AgencySystems
  ): Promise<void> => {
    if (file && system && userStore.currentAgencyId) {
      const formData = new FormData();
      formData.append("file", file);
      formData.append("name", file.name);
      formData.append("system", system);
      formData.append("ingest_on_upload", "True");
      formData.append("agency_id", userStore.currentAgencyId.toString());

      const response = await reportStore.uploadExcelSpreadsheet(formData);
      setIsLoading(false);

      if (response instanceof Error) {
        setUploadError(true);
        return showToast("Failed to upload. Please try again.", false, "red");
      }

      setUploadError(false);
      showToast(
        "File uploaded successfully and is pending processing by a Justice Counts administrator.",
        true,
        undefined,
        3500
      );
      /** Placeholder - this should navigate to the confirmation component */
      navigate("/");
    }
  };

  const handleSystemSelection = (file: File, system: AgencySystems) => {
    setIsLoading(true);
    setSelectedSystem(system);
    handleFileUpload(file, system);
    setSelectedFile(undefined);
  };

  if (isLoading) {
    return (
      <DataUploadContainer>
        <Loading />
      </DataUploadContainer>
    );
  }

  const renderCurrentUploadStep = (): JSX.Element => {
    if (selectedFile) {
      /** System Selection Step (for multi-system users) */
      return (
        <SystemSelection
          selectedFile={selectedFile}
          userSystems={userSystems}
          handleSystemSelection={handleSystemSelection}
        />
      );
    }

    /** Upload Error/Warnings Step */
    if (uploadError) {
      /** This object is temporary for the purpose of displaying each UI state */
      const mockErrors = [
        {
          metricTitle: "Releases",
          errorsAndWarnings: [
            {
              type: "error",
              errorTitle: "Breakdown not recognized",
              errorDescription: "Label Not Recognized",
            },
            {
              type: "error",
              errorTitle: "Breakdown not recognized",
              errorDescription: "Label Not Recognized",
            },
            {
              type: "error",
              errorTitle: "Breakdown not recognized",
              errorDescription: "Label Not Recognized",
            },
            {
              type: "warning",
              errorTitle: "Breakdown not recognized",
              errorDescription: "Label Not Recognized",
            },
          ],
        },
        {
          metricTitle: "Admissions",
          errorsAndWarnings: [
            {
              type: "error",
              errorTitle: "Missing value",
              errorDescription: "August 2022: Total",
              additionalInfo:
                "The total value for Admissions will be shown as the sum of the breakdowns.",
            },
            {
              type: "error",
              errorTitle: "Breakdown not recognized",
              errorDescription: "Label Not Recognized",
            },
            {
              type: "error",
              errorTitle: "Breakdown not recognized",
              errorDescription: "Label Not Recognized",
            },
            {
              type: "warning",
              errorTitle: "Breakdown not recognized",
              errorDescription: "Label Not Recognized",
            },
          ],
        },
      ];

      const systemFileName =
        selectedSystem && systemToTemplateSpreadsheetFileName[selectedSystem];

      return (
        <UserPromptContainer>
          <UserPromptWrapper>
            <FileName error>
              <ErrorIcon />
              File Name.xls
            </FileName>
            <UserPromptTitle>Uh oh, we found 4 errors.</UserPromptTitle>
            <UserPromptDescription>
              We ran into a few discrepancies between the uploaded data and the
              Justice Counts format for the{" "}
              <span>
                <a
                  href={`./assets/${systemFileName}`}
                  download={systemFileName}
                >
                  {selectedSystem &&
                    removeSnakeCase(selectedSystem).toLowerCase()}
                </a>
              </span>{" "}
              system. To continue, please resolve the errors in your file and
              reupload.
            </UserPromptDescription>

            <ButtonWrapper>
              <Button type="blue" onClick={() => setUploadError(false)}>
                New Upload
              </Button>
            </ButtonWrapper>

            <UserPromptErrorContainer>
              {mockErrors.map((item) => (
                <UserPromptError key={item.metricTitle}>
                  <MetricTitle>{item.metricTitle}</MetricTitle>

                  {item.errorsAndWarnings?.map((errorItem) => (
                    <Fragment key={errorItem.errorDescription}>
                      <ErrorIconWrapper>
                        {errorItem.type === "error" ? (
                          <ErrorIcon />
                        ) : (
                          <WarningIcon />
                        )}

                        <ErrorMessageWrapper>
                          <ErrorMessageTitle>
                            {errorItem.errorTitle}
                          </ErrorMessageTitle>
                          <ErrorMessageDescription>
                            {errorItem.errorDescription}
                          </ErrorMessageDescription>
                        </ErrorMessageWrapper>
                      </ErrorIconWrapper>
                      <ErrorAdditionalInfo>
                        {errorItem.additionalInfo}
                      </ErrorAdditionalInfo>
                    </Fragment>
                  ))}
                </UserPromptError>
              ))}
            </UserPromptErrorContainer>
          </UserPromptWrapper>
        </UserPromptContainer>
      );
    }

    /** Upload File Step */
    return (
      <UploadFile
        userSystems={userSystems}
        setIsLoading={setIsLoading}
        setSelectedFile={setSelectedFile}
        handleFileUpload={handleFileUpload}
      />
    );
  };

  return (
    <DataUploadContainer>
      <DataUploadHeader transparent={!selectedFile}>
        <LogoContainer onClick={() => navigate("/")}>
          <Logo src={logoImg} alt="" />
        </LogoContainer>

        <Button
          type={selectedFile ? "red" : "light-border"}
          onClick={() => navigate(-1)}
        >
          Cancel
        </Button>
      </DataUploadHeader>

      {renderCurrentUploadStep()}
    </DataUploadContainer>
  );
});
