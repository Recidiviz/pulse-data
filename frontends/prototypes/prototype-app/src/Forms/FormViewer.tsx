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

import { Button, palette } from "@recidiviz/design-system";
import html2canvas from "html2canvas";
import { jsPDF } from "jspdf";
import { rem } from "polished";
import * as React from "react";
import styled from "styled-components/macro";

import { PDF_DPI, PrintablePage } from "./US_TN/styles";

const FormViewerControls = styled.div`
  padding: ${rem(32)};
  display: flex;
  justify-content: space-between;
`;

const FormViewerGrid = styled.div`
  background-color: ${palette.slate10};
  border-left: 1px solid ${palette.slate20};
  display: grid;
  grid-template-rows: min-content 1fr;
`;

interface FormViewerProps {
  fileName: string;
}

const FormViewer: React.FC<FormViewerProps> = ({ fileName, children }) => {
  const formRef = React.useRef() as React.MutableRefObject<HTMLDivElement>;

  const download = () => {
    if (!formRef.current) {
      return;
    }

    const pages = Array.from(
      formRef.current?.querySelectorAll(`${PrintablePage}`)
    ) as HTMLElement[];

    // eslint-disable-next-line new-cap
    const pdf = new jsPDF({
      unit: "in",
      format: [8.5, 11],
    });
    const margin = 0.75;

    Promise.all(pages.map((page) => html2canvas(page))).then((canvases) => {
      Array.from(document.querySelectorAll("canvas")).forEach((canvas) =>
        canvas.parentNode?.removeChild(canvas)
      );

      canvases.forEach((canvas, index) => {
        if (index > 0) {
          pdf.addPage();
        }

        const imgData = canvas.toDataURL("image/jpeg", 1.0);
        pdf.addImage(
          imgData,
          "JPEG",
          margin,
          margin,
          pages[index].offsetWidth / PDF_DPI - margin * 2,
          pages[index].offsetHeight / PDF_DPI - margin * 2
        );
      });

      pdf.save(fileName);
    });
  };

  return (
    <FormViewerGrid>
      <FormViewerControls>
        <Button kind="primary" shape="pill" onClick={download}>
          Download form
        </Button>
      </FormViewerControls>

      <div ref={formRef}>{children}</div>
    </FormViewerGrid>
  );
};

export default FormViewer;
