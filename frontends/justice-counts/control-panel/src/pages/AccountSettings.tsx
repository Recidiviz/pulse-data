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
import { useNavigate } from "react-router-dom";
import styled from "styled-components/macro";

import {
  Button,
  GoBack,
  TextInput,
  Title,
  TitleWrapper,
} from "../components/Forms";
import { useStore } from "../stores";

const AccountSettingsPage = styled.div`
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

const SettingsFormPanel = styled.div`
  width: 644px;
`;

const ButtonWrapper = styled.div`
  display: flex;
  flex: 1 1 auto;
  justify-content: space-between;
`;

const AccountSettings = () => {
  const { userStore } = useStore();
  const navigate = useNavigate();
  const [email, setEmail] = React.useState<string>(userStore?.email || "");
  const [name, setName] = React.useState<string>(userStore?.name || "");
  return (
    <AccountSettingsPage>
      <GoBack
        style={{ position: "absolute", top: 100, left: 20 }}
        onClick={() => navigate(-1)}
      />
      <SettingsFormPanel>
        <TitleWrapper underlined>
          <Title>Settings</Title>
        </TitleWrapper>

        <TextInput
          label="Full Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <TextInput
          label="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />

        <ButtonWrapper>
          <Button
            onClick={() => {
              navigate(-1);
            }}
          >
            Cancel
          </Button>
          <Button
            onClick={() => {
              userStore.updateUserNameAndEmail(name, email);
              navigate(-1);
            }}
          >
            Save & Close
          </Button>
        </ButtonWrapper>
      </SettingsFormPanel>
    </AccountSettingsPage>
  );
};

export default AccountSettings;
