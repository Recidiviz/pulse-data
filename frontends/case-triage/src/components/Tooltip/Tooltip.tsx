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
import * as ReactDOM from "react-dom";
import TooltipElement, { TooltipState } from "./Tooltip.styles";

interface TooltipProps {
  children: React.ReactChild | React.ReactChild[];
  title: React.ReactNode;
}

const Tooltip: React.FC<TooltipProps> = ({ children, title }: TooltipProps) => {
  const [offset, setOffset] = React.useState({ top: "0px", left: "0px" });
  const [shouldRenderTooltip, setShouldRenderTooltip] = React.useState(false);
  const [animationState, setAnimationState] =
    React.useState<TooltipState>(null);
  const [timeoutID, setTimeoutID] = React.useState<number>(-1);

  let frame: number;
  const onMouseMove: React.MouseEventHandler<HTMLDivElement> = (event) => {
    if (typeof frame !== "undefined") {
      window.cancelAnimationFrame(frame);
    }
    frame = window.requestAnimationFrame(() => {
      setOffset({
        left: `${event.clientX + 15}px`,
        top: `${event.clientY + 15}px`,
      });
    });
  };

  React.useEffect(() => {
    return () => window.clearTimeout(timeoutID);
  }, [timeoutID]);

  const onMouseEnter = () => {
    setShouldRenderTooltip(true);
    window.clearTimeout(timeoutID);
    const timeout = window.setTimeout(() => {
      setAnimationState("entering");
    }, 1000);
    setTimeoutID(timeout);
  };

  const onMouseLeave = () => {
    setAnimationState("exiting");
    window.clearTimeout(timeoutID);
    const timeout = window.setTimeout(() => {
      setShouldRenderTooltip(false);
    }, 300);
    setTimeoutID(timeout);
  };

  let tooltip = null;
  if (shouldRenderTooltip) {
    tooltip = ReactDOM.createPortal(
      <TooltipElement
        state={animationState}
        style={{ top: offset.top, left: offset.left }}
      >
        {title}
      </TooltipElement>,
      window.document.body
    );
  }

  return (
    <>
      {tooltip}

      <div
        onMouseMove={onMouseMove}
        onMouseEnter={onMouseEnter}
        onMouseLeave={onMouseLeave}
      >
        {children}
      </div>
    </>
  );
};

export default Tooltip;
