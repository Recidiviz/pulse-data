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

import React from "react";
import styled from "styled-components/macro";

import { useStore } from "../../stores/StoreProvider";
import logo from "../assets/jc-logo-green-vector.png";
import { palette, typography } from "../GlobalStyles";

export const PageContainer = styled.div`
  display: flex;
  justify-content: center;
  height: 100%;
  align-items: center;
`;

export const ContentContainer = styled.div`
  display: flex;
  flex-direction: column;
  width: 652px;
`;
export const LogoContainer = styled.div`
  display: flex;
`;
export const Logo = styled.img`
  width: 60px;
  height: 60px;
`;
export const LogoTitleContainer = styled.div`
  ${typography.sizeCSS.medium}
  display: flex;
  flex-direction: column;
  padding-left: 10px;
  padding-top: 10px;
`;

export const VerificationPageHeader = styled.div`
  ${typography.sizeCSS.large}
  padding-top: 50px;
`;

export const ParagraphContainer = styled.div`
  ${typography.sizeCSS.medium}
  padding-top: 24px;
  padding-bottom: 32px;
  color: ${palette.solid.grey};
`;

export const JusticeCountsSupportLink = styled.a`
  color: ${palette.solid.green};
`;

export const VerificationLogoutButton = styled.button`
  width: 147px;
  height: 56px;
  display: flex;
  justify-content: center;
  align-items: center;
  background: ${palette.solid.blue};
  border: 1px solid ${palette.highlight.grey3};
  color: ${palette.solid.white};
  font-size: 18px;
  &:hover {
    cursor: pointer;
  }
`;

const VerificationPage = () => {
  const { authStore } = useStore();
  const handleLogout = () => authStore.logoutUser();

  return (
    <PageContainer>
      <ContentContainer>
        <LogoContainer>
          <Logo src={logo} alt="" />
          <LogoTitleContainer>
            <div>Publisher</div>
            <div>By Justice Counts</div>
          </LogoTitleContainer>
        </LogoContainer>
        <VerificationPageHeader>
          Please verify your email.
        </VerificationPageHeader>
        <ParagraphContainer>
          <p>
            If you have just signed up for an account, please check your inbox
            for an email asking you to verify your email address. After you
            click the verification button or link in that email, you can reach
            the home page below.
          </p>
          <br />
          <p>
            If you have reached this page by mistake, please try to log in
            again. If you are still having trouble, please reach out to{" "}
            <JusticeCountsSupportLink href="mailto:humphrey@recidiviz.org?subject=Trouble logging into Justice Counts Publisher Control Panel">
              Justice Counts Support
            </JusticeCountsSupportLink>
            .
          </p>
        </ParagraphContainer>

        <VerificationLogoutButton onClick={handleLogout}>
          Logout
        </VerificationLogoutButton>
      </ContentContainer>
    </PageContainer>
  );
};

export default VerificationPage;
