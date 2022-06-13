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

import checkIconWhite from "../assets/status-check-white-icon.png";
import { palette, typography } from "../GlobalStyles";

type ToastColor = "blue" | "red" | "grey";

export const showToast = (
  message: string,
  check = false,
  color: ToastColor = "blue",
  timeout = 2500,
  preventOverride?: boolean
) => {
  const animationTransform = [{ maxWidth: "0px" }, { maxWidth: "100%" }];
  const animationTransformReverse = [
    { maxWidth: "700px" },
    { maxWidth: "0px" },
  ];

  const activeToast = document.querySelector("#toast");

  if (preventOverride && activeToast?.textContent === message) return;

  if (activeToast) {
    activeToast.animate(animationTransformReverse, {
      duration: 600,
      fill: "forwards",
      easing: "ease",
    });
    document.body.removeChild(activeToast);
  }

  const toastElementWrapper = document.createElement(`div`);
  const toastElement = document.createElement(`div`);
  const checkIcon = document.createElement(`img`);
  toastElement.innerText = message;
  toastElementWrapper.id = "toast";
  toastElementWrapper.style.cssText = `
      position: fixed;
      top: 0;
      left: 65px;
      z-index: 100;
      overflow: hidden;
    `;
  toastElementWrapper.appendChild(toastElement);

  let toastBackgroundColor = palette.solid.blue;
  switch (color) {
    case "red":
      toastBackgroundColor = palette.solid.red;
      break;
    case "grey":
      toastBackgroundColor = palette.solid.white;
      break;
    default:
      break;
  }
  toastElement.style.cssText = `
      width: auto;
      height: 64px;
      display: flex;
      align-items: center;
      background: ${toastBackgroundColor};
      color: ${color === "grey" ? palette.solid.grey : palette.solid.white};
      padding: 20px 24px;
      border-radius: 2px;
      white-space: nowrap;
    `;
  checkIcon.src = checkIconWhite;
  checkIcon.alt = "";
  checkIcon.style.cssText = `
      width: 16px;
      height: 16px;
      margin-right: 8px;
      ${typography.sizeCSS.normal}
    `;

  if (check) toastElement.prepend(checkIcon);
  document.body.appendChild(toastElementWrapper);

  toastElementWrapper.animate(animationTransform, {
    duration: 800,
    fill: "forwards",
    easing: "ease",
  });

  // a timeout of -1 keeps the toast from timing out
  if (timeout !== -1) {
    setTimeout(() => {
      toastElementWrapper.animate(animationTransformReverse, {
        duration: 600,
        fill: "forwards",
        easing: "ease",
      });
      Promise.all(
        toastElementWrapper
          .getAnimations({ subtree: true })
          .map((animation) => animation.finished)
      ).then(() => toastElementWrapper.remove());
    }, timeout);
  }
};
