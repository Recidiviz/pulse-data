import { Button } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import React from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

const LogoutButton = styled(Button)`
  display: inline-block;
`;

const Logout: React.FC = () => {
  const { userStore } = useDataStore();

  return (
    <div>
      {userStore.userName}{" "}
      <LogoutButton
        kind="secondary"
        shape="block"
        onClick={() => userStore.logout()}
      >
        Logout
      </LogoutButton>
    </div>
  );
};

export default observer(Logout);
