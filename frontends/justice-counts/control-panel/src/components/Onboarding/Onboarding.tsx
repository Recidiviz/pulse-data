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

import React, { useEffect, useRef, useState } from "react";
import styled, { keyframes } from "styled-components/macro";

import { useStore } from "../../stores";
import logo from "../assets/jc-logo-vector-onboarding.png";
import { palette, typography } from "../GlobalStyles";
import {
  DATA_ENTRY_WIDTH,
  ONE_PANEL_MAX_WIDTH,
  SIDE_PANEL_WIDTH,
  TWO_PANEL_MAX_WIDTH,
} from "../Reports/ReportDataEntry.styles";
import { showToast } from "../Toast";

export const OnboardingContainer = styled.div`
  width: 100vw;
  height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  position: fixed;
  top: 0;
  left: 0;
  z-index: 99;
  color: ${palette.solid.white};
`;

export const OnboardingBackdropContainer = styled.div`
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  background-color: ${palette.highlight.grey12};
  backdrop-filter: blur(3px);
`;

const OnboardingIntroModal = styled.div`
  width: 825px;
  height: 599px;
  display: flex;
  flex-direction: column;
  position: relative;
  border-radius: 15.5px;
  padding: 59px 48px 59px 59px;
  background: linear-gradient(62.14deg, #007541 50.01%, #029553 100%);
  z-index: 1;
`;

const LogoImage = styled.img`
  width: 441px;
  position: absolute;
  top: 0;
  right: 0;
  z-index: -1;
`;

const OnboardingTitle = styled.div`
  ${typography.sizeCSS.large}
  line-height: 58px;
  text-transform: uppercase;
  margin-bottom: 25px;

  span {
    display: block;
    font-weight: 900;
    font-size: 60px;
    line-height: 34px;
  }
`;

const OnboardingIntroContent = styled.div`
  ${typography.sizeCSS.large}
  color: rgba(223, 246, 236, 1);
  line-height: 36px;
  margin-top: 14px;

  p:not(:last-child) {
    margin-bottom: 34px;
  }

  span {
    font-weight: 700;
  }
`;

const OnboardingIntroGetStartedButton = styled.button`
  ${typography.sizeCSS.title}
  width: 100%;
  height: 96px;
  position: absolute;
  bottom: 0;
  left: 0;
  border: none;
  border-radius: 0 0 15.5px 15.5px;
  background: rgba(2, 95, 53, 1);
  color: ${palette.solid.white};

  &:hover {
    cursor: pointer;
    color: rgba(223, 246, 236, 1);
  }
`;

const float = keyframes`
  0% { transform: translate(0,  0px); }
  50%  { transform: translate(0, 10px); }
  100%   { transform: translate(0, 0px); }   
`;

type OnboardModalPositionProps =
  | "bottomleft"
  | "bottomright"
  | "reportsummary"
  | "publishdata";

const OnboardingModal = styled.div<{
  position: OnboardModalPositionProps;
  modalHeight: number;
  topic: OnboardingTopics;
  lastSection?: boolean;
}>`
  width: 532px;
  height: fit-content;
  position: absolute;
  z-index: 1;
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
  transition: top 0.5s ease, left 0.5s ease, bottom 0.5s ease, right 0.5s ease,
    background 1s ease;
  ${({ position, modalHeight, topic }) => {
    if (position === "bottomleft") {
      return `
        top: calc(100% - ${modalHeight}px - 24px);
        right: calc(100% - 532px - 24px);
        bottom: 24px;
        left: 24px;
      `;
    }
    if (position === "bottomright") {
      return `
        ${
          topic === "reportsview"
            ? `top: unset;`
            : `top: calc(100% - ${modalHeight}px - 24px);`
        }
        left: calc(100% - 532px - 24px);
        bottom: 24px;
        right: 24px;
      `;
    }
    if (position === "reportsummary") {
      return `
        bottom: calc(100% - ${modalHeight}px - 99px);
        right: calc(100% - 532px - ${SIDE_PANEL_WIDTH}px);
        top: 99px;
        left: ${SIDE_PANEL_WIDTH}px;
      `;
    }
    if (position === "publishdata") {
      return `
        bottom: calc(100% - ${modalHeight}px - 113px);
        left: calc(100% - 532px - ${SIDE_PANEL_WIDTH}px);
        top: 113px;
        right: ${SIDE_PANEL_WIDTH}px;
      `;
    }
  }}
  border-radius: 10px;
  padding: 38px 31px 97px 38px;
  background: ${({ lastSection }) =>
    lastSection ? palette.solid.green : palette.solid.blue};
  box-shadow: 0px 4px 10px rgba(53, 83, 98, 0.4);
  animation: ${float} 3s infinite ease-in-out;

  @media only screen and (max-width: ${TWO_PANEL_MAX_WIDTH}px) {
    ${({ position, modalHeight }) => {
      if (position === "publishdata") {
        return `
          top: calc(100% - ${modalHeight}px - 28px);
          right: calc(100% - 532px - ${SIDE_PANEL_WIDTH}px);
          left: ${SIDE_PANEL_WIDTH}px;
        `;
      }
      return ``;
    }}
  }
`;

const OnboardingModalTitle = styled.div`
  ${typography.sizeCSS.large}
  font-weight: 700;
`;

const OnboardingModalContent = styled.div`
  ${typography.sizeCSS.medium}
  line-height: 27px;
  margin-top: 11px;

  a {
    color: ${palette.solid.white};
  }
`;

const OnboardingModalActionButtonWrapper = styled.div<{
  lastSection?: boolean;
}>`
  ${typography.sizeCSS.large}
  height: 62px;
  width: 100%;
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding-right: 31px;
  position: absolute;
  left: 0;
  bottom: 0;
  border-radius: 0px 0px 10px 10px;
  background: ${({ lastSection }) =>
    lastSection
      ? `linear-gradient(90deg, rgba(5, 101, 57, 0) 0%, #005931 100%)`
      : `linear-gradient(90deg, rgba(0, 69, 137, 0) 0%, #004589 100%)`};
`;

const OnboardingProgressIndicatorWrapper = styled.div`
  margin-right: 17px;
  display: flex;
`;

const OnboardingProgressIndicator = styled.div<{ filled?: boolean }>`
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background: ${({ filled }) =>
    filled ? palette.solid.white : `rgba(255, 255, 255, 0.3)`};
  margin: 9px;

  &:hover {
    cursor: pointer;
  }
`;

type OnboardingActionButtonProps = "Next" | "Finish" | "Close";

const OnboardingActionButton = styled.button<{
  action: OnboardingActionButtonProps;
}>`
  ${typography.sizeCSS.large}
  background: none;
  border: none;
  color: ${palette.solid.white};

  &::after {
    content: ${({ action }) => {
      if (action === "Next") {
        return `"Next →"`;
      }
      if (action === "Finish") {
        return `"Finish →"`;
      }
      if (action === "Close") {
        return `"Close"`;
      }
    }};
  }

  &:hover {
    cursor: pointer;
    opacity: 0.9;
  }
`;

type OnboardingFadedContainerProps = {
  position: string;
  currentSectionPosition?: string;
};

const OnboardingFadedContainer = styled.div<OnboardingFadedContainerProps>`
  height: 100%;
  background: ${palette.solid.white};
  opacity: 0.7;
  position: absolute;
  top: 0;
  ${({ position }) => {
    if (position === "left") {
      return `
        width: ${SIDE_PANEL_WIDTH}px;
        left: 0;
      `;
    }
    if (position === "right") {
      return `
        width: ${SIDE_PANEL_WIDTH}px;
        right: 0;
      `;
    }
    if (position === "center") {
      return `
        width: ${DATA_ENTRY_WIDTH}px; 
      `;
    }
  }};

  @media only screen and (max-width: ${TWO_PANEL_MAX_WIDTH}px) {
    ${({ position, currentSectionPosition }) => {
      if (position === "left" && currentSectionPosition === "publishdata") {
        return `
          display: none;
        `;
      }
      if (position === "right") {
        return `
          display: none;
        `;
      }
      if (position === "center") {
        return `
          margin-left: 336px;
        `;
      }
    }};
  }

  @media only screen and (max-width: ${ONE_PANEL_MAX_WIDTH}px) {
    ${({ position }) => {
      if (position === "right" || position === "left") {
        return `
          display: none;
        `;
      }
    }};
  }
`;

type OnboardingTopics = "reportsview" | "dataentryview";

const OnboardingIntro = ({ closeIntro }: { closeIntro: () => void }) => {
  return (
    <OnboardingBackdropContainer>
      <OnboardingIntroModal>
        <LogoImage src={logo} alt="" />
        <OnboardingTitle>
          Welcome to <span>Publisher.</span>
        </OnboardingTitle>
        <OnboardingIntroContent>
          <p>
            <span>Welcome to the Justice Counts Publisher!</span> Our team is
            excited that your agency is leading the charge towards a more
            transparent criminal justice system.
          </p>

          <p>
            We’ve built the Publisher Tool to make it as quick and easy for you
            to publish the Justice Counts Metrics on a regular basis. Let’s walk
            through the application.
          </p>
        </OnboardingIntroContent>
        <OnboardingIntroGetStartedButton onClick={closeIntro}>
          Let’s Get Started
        </OnboardingIntroGetStartedButton>
      </OnboardingIntroModal>
    </OnboardingBackdropContainer>
  );
};

type OnboardingSections = {
  order: number | null;
  title: string;
  html: JSX.Element;
  position: OnboardModalPositionProps;
  action: OnboardingActionButtonProps;
};

const OnboardingSessionView = ({
  completeOnboarding,
  topic,
}: {
  completeOnboarding: () => void;
  topic: OnboardingTopics;
}) => {
  const [currentSectionIndex, setCurrentSectionIndex] = useState(0);
  const [currentModalHeight, setCurrentModalHeight] = useState(305);
  const modalHeightRef = useRef() as React.MutableRefObject<HTMLDivElement>;
  const reportsViewSections: OnboardingSections[] = [
    {
      order: 1,
      title: "Reports View",
      html: (
        <>
          <p>
            Every month, we’ll generate new <strong>Reports</strong> on this
            homepage. As a member of this agency’s Justice Counts team, you’ll
            be able to add data for the{" "}
            <a
              href="https://justicecounts.csgjusticecenter.org/metrics/justice-counts-metrics/"
              rel="noreferrer noopener"
              target="_blank"
            >
              <strong>Justice Counts Metrics</strong>
            </a>{" "}
            via these reports whenever the relevant data is ready on your end.
          </p>
        </>
      ),
      position: "bottomright",
      action: "Next",
    },
    {
      order: 2,
      title: "Settings",
      html: (
        <>
          <p>
            If you need to edit your name or email, click on your name and
            access your Settings in the dropdown.
          </p>
          <p>
            Admins can also use these settings to add new team members to
            Publisher.
          </p>
        </>
      ),
      position: "bottomright",
      action: "Next",
    },
    {
      order: 3,
      title: "Feedback",
      html: (
        <>
          <p>
            Your feedback helps us to make this product even easier to use. If
            you have questions, requests, or any other feedback, please email us
            at{" "}
            <a href="mailto:support@justice-counts.org">
              support@justice-counts.org
            </a>{" "}
            and we’ll be happy to get back to you.
          </p>
        </>
      ),
      position: "bottomright",
      action: "Finish",
    },
    {
      order: null,
      title: "You're ready to go!",
      html: (
        <>
          <p>
            If you have any questions, email the Justice Counts team at{" "}
            <a href="mailto:support@justice-counts.org">
              support@justice-counts.org
            </a>
            .
          </p>
        </>
      ),
      position: "bottomright",
      action: "Close",
    },
  ];
  const dataEntryViewSections: OnboardingSections[] = [
    {
      order: 1,
      title: "Entering data",
      html: (
        <>
          <p>
            This is where you’ll enter the data and important contextual
            information about the Justice Counts Metrics (linked{" "}
            <a
              href="https://justicecounts.csgjusticecenter.org/metrics/justice-counts-metrics/"
              rel="noreferrer noopener"
              target="_blank"
            >
              here
            </a>{" "}
            in case you need a refresher). We’ve included additional
            descriptions and definitions on the side in case it’s helpful.
          </p>
        </>
      ),
      position: "bottomright",
      action: "Next",
    },
    {
      order: 2,
      title: "Report Summary",
      html: (
        <>
          <p>
            The metrics associated with each report are listed in the Report
            Summary. Checkmarks indicate that all inputs are valid; X’s indicate
            that there is an error with one of the inputs.
          </p>
          <br />
          <p>
            You can click on a metric to skip to that part in the form, or
            scroll to the metric - either works!
          </p>
        </>
      ),
      position: "reportsummary",
      action: "Next",
    },
    {
      order: 3,
      title: "Publish your data",
      html: (
        <>
          <p>
            Whenever you’re ready, you can publish your data with this button.
            Don’t worry – you’ll still have a chance to review all the data
            before it goes live*, and you can still edit the data even after
            it’s been published.
          </p>
          <br />
          <p>
            *<strong>NOTE</strong>: At this stage, data will just be marked as
            published and will not be live just yet.
          </p>
        </>
      ),
      position: "publishdata",
      action: "Finish",
    },
    {
      order: null,
      title: "You're ready to go!",
      html: (
        <>
          <p>
            If you have any questions, email the Justice Counts team at{" "}
            <a href="mailto:support@justice-counts.org">
              support@justice-counts.org
            </a>
            .
          </p>
        </>
      ),
      position: "bottomright",
      action: "Close",
    },
  ];
  const currentSections =
    topic === "reportsview" ? reportsViewSections : dataEntryViewSections;

  const goToNextSection = () =>
    setCurrentSectionIndex((prev) =>
      prev + 1 < currentSections.length ? prev + 1 : prev
    );

  useEffect(() => {
    if (topic === "reportsview") {
      showToast(
        "Welcome! Click “Next” in the onboarding boxes to continue.",
        false,
        undefined,
        -1,
        true
      );
      return () => showToast("You're ready to go!", false, undefined, 0, false);
    }
  }, [topic]);

  useEffect(() => {
    if (
      topic === "reportsview" &&
      currentSectionIndex + 1 === currentSections.length
    ) {
      showToast("You're ready to go!", false, undefined, -1, true);
    }
  }, [currentSectionIndex, currentSections.length, topic]);

  useEffect(() => {
    if (modalHeightRef.current) {
      setCurrentModalHeight(
        modalHeightRef.current.getBoundingClientRect().height
      );
    }
  }, [currentSectionIndex]);

  return (
    <>
      <OnboardingModal
        position={currentSections[currentSectionIndex].position}
        lastSection={currentSectionIndex + 1 === currentSections.length}
        ref={modalHeightRef}
        modalHeight={currentModalHeight}
        topic={topic}
      >
        <OnboardingModalTitle>
          {currentSections[currentSectionIndex].title}
        </OnboardingModalTitle>

        <OnboardingModalContent>
          {currentSections[currentSectionIndex].html}
        </OnboardingModalContent>

        <OnboardingModalActionButtonWrapper
          lastSection={currentSectionIndex + 1 === currentSections.length}
        >
          <OnboardingProgressIndicatorWrapper>
            {currentSectionIndex + 1 < currentSections.length &&
              currentSections.map(
                (section, index) =>
                  section.order && (
                    <OnboardingProgressIndicator
                      key={section.title}
                      filled={section.order === currentSectionIndex + 1}
                      onClick={() => setCurrentSectionIndex(index)}
                    />
                  )
              )}
          </OnboardingProgressIndicatorWrapper>

          <OnboardingActionButton
            action={currentSections[currentSectionIndex].action}
            onClick={
              currentSections[currentSectionIndex].action === "Close"
                ? completeOnboarding
                : goToNextSection
            }
          />
        </OnboardingModalActionButtonWrapper>
      </OnboardingModal>

      {topic === "dataentryview" &&
        currentSections[currentSectionIndex].position === "bottomright" &&
        currentSections[currentSectionIndex].order && (
          <>
            <OnboardingFadedContainer position="left" />
            <OnboardingFadedContainer position="right" />
          </>
        )}

      {topic === "dataentryview" &&
        currentSections[currentSectionIndex].position === "reportsummary" && (
          <>
            <OnboardingFadedContainer position="center" />
            <OnboardingFadedContainer position="right" />
          </>
        )}

      {topic === "dataentryview" &&
        currentSections[currentSectionIndex].position === "publishdata" && (
          <>
            <OnboardingFadedContainer
              position="left"
              currentSectionPosition="publishdata"
            />
            <OnboardingFadedContainer position="center" />
          </>
        )}
    </>
  );
};

const Onboarding = ({
  setShowOnboarding,
  topic,
}: {
  setShowOnboarding: React.Dispatch<React.SetStateAction<boolean>>;
  topic: OnboardingTopics;
}) => {
  const { userStore } = useStore();
  const [showIntro, setShowIntro] = useState(true);
  const closeIntro = () => setShowIntro(false);
  const completeOnboarding = () => {
    userStore.updateOnboardingStatus(topic, true);
    setShowOnboarding(false);
  };

  /** Prevent body from scrolling when this dialog is open */
  useEffect(() => {
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = "unset";
    };
  }, []);

  return (
    <OnboardingContainer>
      {showIntro && topic === "reportsview" && (
        <OnboardingIntro closeIntro={closeIntro} />
      )}
      {(!showIntro || topic === "dataentryview") && (
        <OnboardingSessionView
          completeOnboarding={completeOnboarding}
          topic={topic}
        />
      )}
    </OnboardingContainer>
  );
};

export default Onboarding;
