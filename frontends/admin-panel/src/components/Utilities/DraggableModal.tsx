// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2023 Recidiviz, Inc.
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

import { Modal } from "antd";
import { ReactNode, useRef, useState } from "react";
import Draggable, { DraggableData, DraggableEvent } from "react-draggable";

interface DraggableModalProps {
  visible: boolean;
  title: string;
  onCancel: () => void;
  onOk?: () => void;
  okText?: string;
  footer?: ReactNode;
  width?: string | number;
}

export const DraggableModal: React.FC<DraggableModalProps> = ({
  visible,
  title,
  onCancel,
  onOk,
  okText = "Add",
  footer,
  width,
  children,
}) => {
  const [disabled, setDisabled] = useState(false);
  const [bounds, setBounds] = useState({
    left: 0,
    top: 0,
    bottom: 0,
    right: 0,
  });
  const dragRef = useRef<HTMLInputElement>(null);
  const onStart = (_event: DraggableEvent, uiData: DraggableData) => {
    const { clientWidth, clientHeight } = window.document.documentElement;
    const targetRect = dragRef.current?.getBoundingClientRect();
    if (!targetRect) {
      return;
    }
    setBounds({
      left: -targetRect.left + uiData.x,
      right: clientWidth - (targetRect.right - uiData.x),
      top: -targetRect.top + uiData.y,
      bottom: clientHeight - (targetRect.bottom - uiData.y),
    });
  };

  return (
    <Modal
      visible={visible}
      title={
        <div
          style={{
            width: "100%",
            cursor: "move",
          }}
          onMouseOver={() => {
            setDisabled(false);
          }}
          onMouseOut={() => {
            setDisabled(true);
          }}
          onFocus={() => undefined}
          onBlur={() => undefined}
        >
          {title}
        </div>
      }
      okText={okText}
      cancelText="Cancel"
      onCancel={onCancel}
      onOk={onOk}
      footer={footer}
      width={width}
      // eslint-disable-next-line react/no-unstable-nested-components
      modalRender={(modal) => (
        <Draggable
          disabled={disabled}
          bounds={bounds}
          onStart={(event, uiData) => onStart(event, uiData)}
        >
          <div ref={dragRef}>{modal}</div>
        </Draggable>
      )}
      bodyStyle={{ overflowY: "scroll", maxHeight: "calc(100vh - 200px)" }}
    >
      {children}
    </Modal>
  );
};
