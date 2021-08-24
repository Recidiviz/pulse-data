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
import styled from "styled-components/macro";
import { rem } from "polished";
import { Button, fonts, palette, spacing } from "@recidiviz/design-system";
import { CarouselControls } from "../Carousel";
import { device } from "../styles";

export const GetStartedButton = styled(Button)`
  font-size: ${rem(16)};

  @media screen and ${device.tablet} {
    height: ${rem(72)};
  }
`;

export const OnboardingText = styled.div`
  flex: 1 1 auto;
  min-width: 100%;

  @media screen and ${device.tablet} {
    min-width: auto;
    width: 50%;

    margin-right: ${rem(110)};
  }
`;

export const OnboardingImageContainer = styled.div`
  display: flex;
  flex: 1 1 auto;
  min-width: 100%;
  align-items: center;

  @media screen and ${device.tablet} {
    min-width: auto;
    width: 50%;
  }
`;

export const OnboardingHeadingText = styled.h1`
  font-size: ${rem(20)};
  font-family: ${fonts.heading};
  font-weight: normal;
  margin-top: ${rem(40)};

  @media screen and ${device.tablet} {
    margin-top: ${rem(80)};
    font-size: ${rem(24)};
  }
`;

export const OnboardingBodyText = styled.div`
  font-size: ${rem(16)};
  font-family: ${fonts.body};
  font-weight: normal;

  margin-top: ${rem(24)};
  margin-bottom: ${rem(40)};

  @media screen and ${device.tablet} {
    font-size: ${rem(18)};
    line-height: 1.7;
    margin-bottom: ${rem(80)};
  }
`;

export const OnboardingImage = styled.img`
  box-shadow: 0 ${rem(15)} ${rem(40)} 0 ${palette.slate30};
  border: ${rem(1)} solid ${palette.slate10};
  width: 100%;
  pointer-events: none;
  user-select: none;
`;

export const OnboardingSlide = styled.div`
  display: flex;
  flex-wrap: wrap;

  @media screen and ${device.tablet} {
    flex-wrap: nowrap;
  }
`;

export const OnboardingCarouselContainer = styled.div`
  flex: 1;
  position: relative;
  padding: 0 ${rem(spacing.xl)};
  max-width: 1288px;
  margin: 0 auto;
  width: 100%;
`;

export const OnboardingCarouselControls = styled(CarouselControls)`
  display: flex;

  // IE11 does not support the hover media query, only hide when we know for sure we're on a touch device
  @media screen and (hover: none) {
    display: none;
  }
`;

export const OnboardingFooter = styled.div`
  display: flex;
  justify-content: space-between;
  margin-top: ${rem(40)};
  margin-bottom: ${rem(40)};

  @media screen and ${device.tablet} {
    margin-top: ${rem(80)};
    margin-bottom: 0;
  }
`;
