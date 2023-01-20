import { Button } from "@recidiviz/design-system";
import { observer } from "mobx-react-lite";
import { FC } from "react";
import styled from "styled-components/macro";

import { useDataStore } from "../StoreProvider";

const LogoutButton = styled(Button)`
  display: inline-block;
`;

const Logout: FC = () => {
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
