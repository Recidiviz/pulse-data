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

import React, { useState } from "react";
import styled from "styled-components/macro";

import { palette } from "../components/GlobalStyles";

const PreviewButton = styled.button<{ open?: boolean }>`
  height: 80px;
  width: 80px;
  position: fixed;
  bottom: 15px;
  right: 15px;
  z-index: 1000;
  background: ${palette.solid.blue};
  color: ${palette.solid.white};
  border: none;
  border-radius: 50%;
  opacity: ${({ open }) => (open ? 1 : 0)};
  transition: 0.2s ease;

  &:hover {
    cursor: pointer;
    opacity: 1;
  }
`;

const PreviewDataObject: React.FC<{
  description?: string;
  objectToDisplay: Record<string, unknown>;
}> = ({ description, objectToDisplay }) => {
  const [open, setOpen] = useState(false);

  return (
    <>
      <PreviewButton onClick={() => setOpen(!open)} open={open}>
        {open ? "Close" : "Preview"}
      </PreviewButton>
      {open && (
        <pre
          style={{
            position: "fixed",
            height: 550,
            width: 800,
            background: "white",
            fontSize: "0.8rem",
            border: "1px solid #000",
            boxShadow: "1px 1px 6px #aaa",
            borderRadius: 10,
            zIndex: 2000,
            padding: 15,
            overflow: "scroll",
          }}
        >
          <p
            style={{
              fontSize: "0.9rem",
              fontFamily: "Arial",
              fontWeight: 600,
              marginBottom: 10,
              color: palette.solid.blue,
            }}
          >
            {description}
          </p>
          {JSON.stringify(objectToDisplay, null, 2)}
        </pre>
      )}
    </>
  );
};

export default PreviewDataObject;
