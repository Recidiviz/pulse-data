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
import { Link, RouteComponentProps } from "@reach/router";
import {
  Assets,
  Header,
  palette,
  Sans14,
  Sans18,
  spacing,
  typography,
} from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import { rem } from "polished";
import React, { ReactElement } from "react";
import styled, { createGlobalStyle } from "styled-components/macro";
import AuthWall from "../components/AuthWall";
import UserSection from "../components/UserSection";

import ATLAS from "../assets/atlas.png";

const Container = styled.div`
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  max-width: ${rem(422)};
  padding: 0 ${rem(spacing.xl)};
  margin: 0 auto;
  overflow: hidden;
  margin-top: -${rem(spacing.xxl)};
  flex: 1;
`;

const Heading = styled(Sans18).attrs({ as: "strong" })`
  display: block;
  margin-top: ${rem(spacing.lg)};
  margin-bottom: ${rem(spacing.sm)};
`;

const Body = styled(Sans14)`
  color: ${palette.slate85};
`;

const Email = styled.a`
  color: ${palette.signal.links};
  margin-top: 1em;
  ${typography.Sans14};
`;

const HangTightGlobalStyle = createGlobalStyle`
body {
  background: white;
}
#root {
  height: 100%;
}

#root > div {
  display: flex;
  flex-direction: column;
}
`;

const HangTight: React.FC<RouteComponentProps> = (): ReactElement => {
  return (
    <>
      <HangTightGlobalStyle />
      <AuthWall>
        <Header
          left={
            <Link to="/">
              <img src={Assets.LOGO} alt="Recidiviz - Case Triage" />
            </Link>
          }
          right={<UserSection />}
        />
        <Container>
          <img
            src={ATLAS}
            alt="Recidiviz and ATLAS"
            style={{ marginRight: rem(64), width: rem(422) }}
          />
          <Heading>The ATLAS migration is heavy</Heading>
          <Body>
            We’re catching up with the ATLAS migration over the holidays. We
            promise we’ll be back soon! Let us know if there are new things
            you’d love to see in the future.
          </Body>
          <Email href="mailto:feedback@recidiviz.org">
            feedback@recidiviz.org
          </Email>
        </Container>
      </AuthWall>
    </>
  );
};

export default observer(HangTight);
