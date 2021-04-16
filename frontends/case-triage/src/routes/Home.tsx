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
import React, { ReactElement } from "react";
import styled from "styled-components/macro";
import { rem } from "polished";
import { observer } from "mobx-react-lite";

import { Assets, Header, Search, spacing } from "@recidiviz/design-system";
import AuthWall from "../components/AuthWall";
import CaseCard from "../components/CaseCard";
import ClientList from "../components/ClientList";
import UserSection from "../components/UserSection";
import { useRootStore } from "../stores";
import {
  trackSearchBarEnterPressed,
  trackSearchBarFocused,
} from "../analytics";

const Container = styled.div`
  display: flex;
  justify-content: space-between;
  padding: 0 ${rem(spacing.xl)};
`;

const Left = styled.div`
  width: 700px;
`;

const Right = styled.div`
  padding-left: ${rem(spacing.xl)};
  width: 555px;
`;

const Home = (props: RouteComponentProps): ReactElement => {
  const { clientsStore } = useRootStore();

  const ClientCard =
    clientsStore.activeClient &&
    clientsStore.isVisible(clientsStore.activeClient) ? (
      <CaseCard client={clientsStore.activeClient} />
    ) : null;

  return (
    <AuthWall>
      <Header
        left={
          <Link to="/">
            <img src={Assets.LOGO} alt="Recidiviz - Case Triage" />
          </Link>
        }
        center={
          <Search
            inputClassName="fs-exclude"
            placeholder="Search"
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
              clientsStore.filterClients(e.target.value);
            }}
            onPressEnter={(e: React.KeyboardEvent<HTMLInputElement>) => {
              trackSearchBarEnterPressed((e.target as HTMLInputElement).value);
            }}
            onFocus={trackSearchBarFocused}
            value={clientsStore.clientSearchString}
          />
        }
        right={<UserSection />}
      />
      <Container>
        <Left>
          <ClientList />
        </Left>
        <Right>{ClientCard}</Right>
      </Container>
    </AuthWall>
  );
};

export default observer(Home);
