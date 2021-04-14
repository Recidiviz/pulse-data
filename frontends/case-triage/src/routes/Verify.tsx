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
import { navigate, RouteComponentProps } from "@reach/router";
import React, { ReactElement } from "react";
import styled from "styled-components/macro";
import { rem } from "polished";
import {
  Assets,
  Button,
  Link,
  fonts,
  spacing,
} from "@recidiviz/case-triage-components";

const VerifyHeader = styled.h1`
  font-family: ${fonts.heading};
  font-weight: normal;
  font-style: normal;
  font-size: ${rem(34)};

  margin: ${rem(spacing.xl)} 0;
`;

const VerifyText = styled.p`
  font-family: ${fonts.body};
  font-size: ${rem(19)};
  line-height: ${rem(32)};
`;

const HomeButton = styled(Button)`
  margin: ${rem(spacing.xl)} 0;
`;

const Verify = (props: RouteComponentProps): ReactElement => {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100%",
      }}
    >
      <div style={{ width: 700 }}>
        <img src={Assets.LOGO} alt="Recidiviz - Case Triage" />
        <VerifyHeader>Please verify your email.</VerifyHeader>
        <VerifyText>
          If you have just signed up for an account, please check your inbox for
          an email asking you to verify your email address. After you click the
          verification button or link in that email, you can reach the home page
          below.
        </VerifyText>
        <VerifyText>
          If you have reached this page by mistake, please try to log in again.
          If you are still having trouble, please reach out to{" "}
          <Link href="mailto:web-support@recidiviz.org?subject=Trouble logging into Case Triage">
            Recidiviz Support
          </Link>
          .
        </VerifyText>
        <HomeButton kind="secondary" onClick={() => navigate("/")}>
          Back to Home
        </HomeButton>
      </div>
    </div>
  );
};

export default Verify;
