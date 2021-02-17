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
import { Button, ButtonKind } from "@recidiviz/case-triage-components";
import { User } from "@auth0/auth0-spa-js";
import { useRootStore } from "../../stores";

const UserFlex = styled.div`
  display: flex;
  align-items: center;
`;

const UserName = styled.span`
  font-weight: 500;
  margin: 0 16px;

  display: flex;
  align-items: center;
  text-align: right;
  font-size: ${rem("15px")};
  letter-spacing: -0.01em;
  font-feature-settings: "ss04" on;
`;

const UserAvatar = styled.span`
  background: #006c67;
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
    <Button
      onClick={() => userStore.login && userStore.login()}
      kind={ButtonKind.primary}
    >
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
      <LoginButton />
      {isAuthorized && user ? renderUser({ user }) : null}
    </UserFlex>
  );
};

export default observer(UserSection);
