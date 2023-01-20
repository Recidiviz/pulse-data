import { Button } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import { FC } from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

const LogoutButton = styled(Button)`
  display: inline-block;
`;

const Logout: FC = () => {
  const {
    userStore: { userName },
    authStore,
  } = useDataStore();

  return (
    <div>
      {userName}{" "}
      <LogoutButton
        kind="secondary"
        shape="block"
        onClick={() => authStore.logout()}
      >
        Logout
      </LogoutButton>
    </div>
  );
};

export default observer(Logout);
