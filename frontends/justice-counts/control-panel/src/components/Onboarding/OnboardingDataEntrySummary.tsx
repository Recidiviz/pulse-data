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

import React, { useEffect, useState } from "react";
import styled from "styled-components/macro";

import closeIcon from "../assets/dark-close-icon.png";
import { ReactComponent as Logo } from "../assets/jc-logo-vector.svg";
import { palette, typography } from "../GlobalStyles";
import { ONE_PANEL_MAX_WIDTH } from "../Reports/ReportDataEntry.styles";
import { OnboardingBackdropContainer, OnboardingContainer } from "./Onboarding";

const OnboardingSummaryModal = styled.div`
  width: 60%;
  height: 76%;
  background: ${palette.solid.white};
  box-shadow: inset 0px 0px 10px rgba(23, 28, 43, 0.5);
  border-radius: 15px;
`;

const ModalHeader = styled.div`
  ${typography.sizeCSS.title};
  height: 119px;
  display: flex;
  justify-content: space-between;
  position: relative;
  z-index: 0;
  background: linear-gradient(270deg, #016fdc 0%, #004589 100%);
  box-shadow: inset 0px 0px 10px rgba(23, 28, 43, 0.5);
  border-radius: 15px 15px 0 0;
  padding: 35px 50px;
  overflow: hidden;
`;

const ModalBody = styled.div`
  ${typography.sizeCSS.medium}
  line-height: 27px;
  padding: 35px 50px;
  color: ${palette.solid.darkgrey};
  max-height: 76%;
  overflow: scroll;

  h1 {
    ${typography.sizeCSS.large}
  }

  p:not(:last-child) {
    margin-bottom: 18px;
  }

  ul {
    margin-left: 28px;
    margin-bottom: 18px;
  }
`;

const CloseIcon = styled.img`
  width: 36px;
  height: 36px;
  filter: brightness(0) invert(1);

  &:hover {
    cursor: pointer;
    opacity: 0.8;
  }
`;

const LogoVector = styled(Logo)`
  width: 524px;
  height: 524px;
  position: absolute;
  z-index: -1;
  top: calc(50% - 524px / 2 + 114.5px);
  left: 50%;
  transform: translateX(-50%);

  path {
    fill: rgba(1, 69, 137, 0.3);
  }
`;

export const OnboardingSummaryButton = styled.div`
  width: 36px;
  height: 36px;
  display: flex;
  justify-content: center;
  align-items: center;
  position: fixed;
  z-index: 2;
  bottom: 25px;
  right: 25px;
  background: ${palette.solid.blue};
  color: ${palette.solid.white};
  border-radius: 50%;

  &:hover {
    cursor: pointer;
    opacity: 0.8;
  }

  @media only screen and (max-width: ${ONE_PANEL_MAX_WIDTH}px) {
    display: none;
  }
`;

export const OnboardingDataEntrySummary: React.FC = () => {
  const [showOnboardingSummary, setShowOnboardingSummary] = useState(false);

  /** Prevent body from scrolling when this dialog is open */
  useEffect(() => {
    if (showOnboardingSummary) {
      document.body.style.overflow = "hidden";
    }
    return () => {
      document.body.style.overflow = "unset";
    };
  }, [showOnboardingSummary]);

  const closeOnboardingSummary = (e: React.MouseEvent) => {
    if (e.target !== e.currentTarget) return;
    setShowOnboardingSummary(false);
  };
  const openOnboardingSummary = () => setShowOnboardingSummary(true);

  const OnboardingSummary: React.FC = () => (
    <OnboardingContainer>
      <OnboardingBackdropContainer onClick={closeOnboardingSummary}>
        <OnboardingSummaryModal>
          <ModalHeader>
            <LogoVector />
            <span>Data Entry</span>
            <CloseIcon
              src={closeIcon}
              alt=""
              onClick={closeOnboardingSummary}
            />
          </ModalHeader>

          <ModalBody>
            <p>
              <em>
                This page is where you can enter the data and important
                contextual information about the Justice Counts Metrics.{" "}
              </em>
            </p>

            <h1>Entering Data</h1>
            <p>
              The purpose of this view is to enter data for a given reporting
              period. You can access all reports from the Reports View.{" "}
            </p>

            <span>
              Each metric is composed of three components:
              <ul>
                <li>
                  <strong>Primary Value</strong>: The top-level value associated
                  with the metric.{" "}
                </li>
                <li>
                  <strong>Breakdowns</strong>: Subdivisions of the primary value
                  (e.g., staff types, race, gender, etc).{" "}
                </li>
                <li>
                  <strong>Context</strong>: Additional written or predefined
                  fields that provide additional information about the metric.{" "}
                </li>
              </ul>
            </span>

            <p>
              We’ve also included additional descriptions and definitions on the
              right-side panel, including the definition of the metric,
              definitions for any terms, and details on how the metric should be
              calculated.{" "}
            </p>

            <h1>Publishing Data</h1>
            <p>
              Whenever you have finished entering data, you can finalize and
              publish data via the Review & Publish button.{" "}
            </p>

            <p>
              Clicking this button will first take you to a page to review the
              numbers that you have entered before publishing. Publishing the
              data will mark the report as “Published” in the Reports View and
              the underlying data will be made available to the public via
              Justice Counts publishing channels* (public dashboard, data feed,
              etc).
            </p>
            <p>
              *<strong>NOTE</strong>: At this stage, data will just be marked as
              published and will not be live just yet.
            </p>

            <p>
              After publishing, you can always go back to edit the numbers.{" "}
            </p>
          </ModalBody>
        </OnboardingSummaryModal>
      </OnboardingBackdropContainer>
    </OnboardingContainer>
  );

  return (
    <>
      <OnboardingSummaryButton onClick={openOnboardingSummary}>
        ?
      </OnboardingSummaryButton>

      {showOnboardingSummary && <OnboardingSummary />}
    </>
  );
};
