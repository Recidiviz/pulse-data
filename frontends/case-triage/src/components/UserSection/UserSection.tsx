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
import * as React from "react";
import styled from "styled-components/macro";
import { rem } from "polished";
import { observer } from "mobx-react-lite";
import { Button, palette } from "@recidiviz/case-triage-components";
import { User } from "@auth0/auth0-spa-js";
import { Link } from "@reach/router";
import { useRootStore } from "../../stores";

const UserFlex = styled.div`
  display: flex;
  align-items: center;
`;

const headerStyleBase = `
  font-weight: 500;

  display: flex;
  align-items: center;
  text-align: right;
  font-size: ${rem("15px")};
  letter-spacing: -0.01em;
  font-feature-settings: "ss04" on;

  color: ${palette.text.normal};
`;

const UserName = styled.span`
  ${headerStyleBase}

  margin: 0 16px;
`;

const FAQLink = styled.a`
  ${headerStyleBase}

  text-decoration: none;
  &:hover {
    text-decoration: underline;
  }
`;

const UserAvatar = styled.span`
  background: ${palette.logo.blue};
  border-radius: 50%;
  color: white;
  display: inline-block;
  font-size: ${rem("11px")};
  line-height: 32px;
  height: 32px;
  text-align: center;
  text-transform: uppercase;
  width: 32px;
`;

const LoginButton: React.FC = () => {
  const { userStore } = useRootStore();
  const { isAuthorized } = userStore;

  if (isAuthorized) {
    return null;
  }

  return (
    <Button onClick={() => userStore.login && userStore.login()} kind="primary">
      Log In
    </Button>
  );
};

interface UserProps {
  user: User;
}

const renderUser = ({ user }: UserProps) => {
  return (
    <>
      <UserName>{user.name}</UserName>
      <UserAvatar>{user.name && user.name[0]}</UserAvatar>
    </>
  );
};

const UserSection = () => {
  const { userStore } = useRootStore();
  const { isAuthorized, isLoading, user } = userStore;

  if (isLoading) {
    return <UserFlex />;
  }

  return (
    <UserFlex className="fs-exclude">
      <FAQLink
        href="https://docs.google.com/document/d/1iqpKkbsnVpl4bTqSICH4UmwPQ65pOjpIH79L47EquaU/edit?usp=sharing"
        target="_blank"
      >
        FAQ
      </FAQLink>
      <LoginButton />
      {isAuthorized && user ? renderUser({ user }) : null}
    </UserFlex>
  );
};

export default observer(UserSection);
