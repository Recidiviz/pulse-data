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

import { Button, H4, spacing } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React, { ReactElement, useState } from "react";
import styled from "styled-components/macro";

import { RequestProps, useStore } from "./stores";

// Note: This entire component is assembled purely for
// testing calls to our backend API, displaying the response, and successfully
// logging out. Everything here will be removed once we've successfully tested
// the api/hello and auth/logout endpoints and redirect.

const TempPageWrapper = styled.div`
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  text-align: center;
`;

const TempResponseContainer = styled.div`
  width: 500px;
  height: 200px;
  display: flex;
  justify-content: center;
  align-items: center;
  border: 1px solid black;
  border-radius: 5px;
  margin: ${spacing.md}px 0;
  overflow: scroll;
`;

interface TempMessageTypes {
  message?: string;
  response?: string;
}

const App: React.FC = (): ReactElement => {
  const [message, setMessage] = useState<TempMessageTypes>({
    message: "Click one of the buttons below to make a request.",
  });
  const { api } = useStore();

  const helloPathProps: RequestProps = { path: "/api/hello", method: "GET" };

  const callAPI = async (pathProps: RequestProps) => {
    try {
      const response = (await api.request(pathProps)) as Response;
      const data = await response.json();

      if (data) setMessage(data);
    } catch (error) {
      if (error instanceof Error) setMessage(error);
    }
  };

  return (
    <TempPageWrapper>
      <H4>Response</H4>
      <TempResponseContainer>
        {message?.message ? message.message : message?.response}
      </TempResponseContainer>
      <Button onClick={() => callAPI(helloPathProps)}>
        Get Protected Message
      </Button>
      <Button
        kind="secondary"
        onClick={() => api.logout()}
        style={{ marginTop: spacing.md }}
      >
        Logout
      </Button>
    </TempPageWrapper>
  );
};

export default observer(App);
