// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2021 Recidiviz, Inc.
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
import { navigate } from "@reach/router";
import { Icon, IconSVG } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React from "react";
import {
  ONBOARDING_DATA,
  ONBOARDING_NEEDS,
  ONBOARDING_WHICH_CLIENT,
} from "../../assets/onboarding";
import { useRootStore } from "../../stores";
import { Carousel, CarouselIndicators } from "../Carousel";
import {
  GetStartedButton,
  OnboardingBodyText,
  OnboardingCarouselControls,
  OnboardingFooter,
  OnboardingHeadingText,
  OnboardingImage,
  OnboardingImageContainer,
  OnboardingSlide,
  OnboardingText,
} from "./OnboardingCarousel.styles";

const OnboardingCarousel: React.FC = () => {
  const { policyStore, officerMetadataStore } = useRootStore();

  if (policyStore.isLoading) {
    return null;
  }

  const skip = async () => {
    await navigate("/");
    await officerMetadataStore.setHasSeenOnboarding(true);
  };

  const slides = [
    {
      image: ONBOARDING_WHICH_CLIENT,
      alt: "Case Triage Product",
      title: "Welcome to Case Triage, a simple way to manage your caseload.",
      body: (
        <>
          This tool was built in partnership with {policyStore.getDOCName()}{" "}
          officers just like you.
        </>
      ),
    },
    {
      image: ONBOARDING_WHICH_CLIENT,
      alt: "Case Triage Client List",
      title: "Prioritize your clients’ needs.",
      body: (
        <>
          View all of your clients and sort by upcoming contacts, upcoming
          assessments, and time on supervision. Start at the top and work your
          way down!
        </>
      ),
    },
    {
      image: ONBOARDING_NEEDS,
      alt: "Case Triage Client View",
      title: "See a snapshot of each client.",
      body: (
        <>
          Stay on top of the details that matter the most, from upcoming risk
          assessments to opportunities like correcting a supervision level
          mismatch. Hover or click on a client to dig into details!
        </>
      ),
    },
    {
      image: ONBOARDING_DATA,
      alt: "Case Triage Data",
      title: "We’d love to hear from you.\n",
      body: (
        <>
          We pull data from {policyStore.omsName} every 1-2 days, but you know
          your caseload best. If you see something off, report it by clicking
          the three dots next to the client’s name or the chat icon (bottom
          right). Thanks for your valuable feedback as we continue to build a
          tool tailored just for you!
        </>
      ),
    },
  ];

  const getStartedButton = (
    <GetStartedButton kind="secondary" onClick={skip}>
      Get started &nbsp; <Icon kind={IconSVG.Arrow} size={12} />
    </GetStartedButton>
  );

  return (
    <Carousel>
      {slides.map((slide, index) => (
        <OnboardingSlide key={slide.title}>
          <OnboardingText>
            <CarouselIndicators />
            <OnboardingHeadingText>{slide.title}</OnboardingHeadingText>
            <OnboardingBodyText>{slide.body}</OnboardingBodyText>
            <OnboardingFooter>
              <OnboardingCarouselControls />
              {index === slides.length - 1 ? getStartedButton : null}
            </OnboardingFooter>
          </OnboardingText>
          <OnboardingImageContainer>
            <OnboardingImage src={slide.image} alt={slide.alt} />
          </OnboardingImageContainer>
        </OnboardingSlide>
      ))}
    </Carousel>
  );
};

export default observer(OnboardingCarousel);
