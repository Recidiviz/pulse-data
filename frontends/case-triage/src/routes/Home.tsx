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
import {
  Assets,
  Button,
  Header,
  Modal,
  Search,
  spacing,
} from "@recidiviz/design-system";
import { rem } from "polished";
import { observer } from "mobx-react-lite";

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
  max-width: 1288px;
  padding: 0 ${rem(spacing.xl)};
  margin: 0 auto;
`;

const Left = styled.div`
  width: 700px;
`;

const Right = styled.div`
  padding-left: ${rem(spacing.xl)};
  width: 555px;
`;

const ReloadButton = styled(Button)`
  margin: ${rem(spacing.lg)} auto 0 auto;
`;

const Home = (props: RouteComponentProps): ReactElement => {
  const { clientsStore, userStore } = useRootStore();

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
      <Modal
        isOpen={userStore.shouldReload}
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        appElement={document.getElementById("root")!}
      >
        <div>
          Please reload the page. A new update is available, and the app may not
          continue to work correctly without refreshing.
        </div>
        <ReloadButton onClick={() => window.location.reload()}>
          Reload
        </ReloadButton>
      </Modal>
    </AuthWall>
  );
};

export default observer(Home);
