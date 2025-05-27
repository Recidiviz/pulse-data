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
import { Button, Form, Input, Modal } from "antd";
import { useState } from "react";

import {
  acquireResourceLocksForStateAndInstance,
  releaseResourceLocksForStateById,
} from "../../AdminPanelAPI/IngestOperations";
import {
  DirectIngestInstance,
  ResourceLockState,
  ResourceLockStatus,
} from "./constants";

export enum LockAction {
  AcquireRawDataResourceLocks = "Acquire Locks",
  ReleaseRawDataResourceLocks = "Release Locks",
}

export type LockActionContext = {
  action: LockAction;
  formValues: unknown;
};

type AcquireLockFormValues = {
  description: string;
  name: string;
};

interface LockActionButtonProps {
  action: LockAction;
  stateCode: string;
  disabled: boolean;
  rawDataInstance: DirectIngestInstance;
  buttonText: string;
  style: React.CSSProperties;
  currentLocks: ResourceLockStatus[];
  onActionConfirmed: () => void;
  lockState: ResourceLockState;
}

const LockActionButton: React.FC<LockActionButtonProps> = ({
  action,
  stateCode,
  disabled,
  rawDataInstance,
  buttonText,
  style,
  currentLocks,
  onActionConfirmed,
  lockState,
}) => {
  const [form] = Form.useForm();
  const [isConfirmationModalVisible, setIsConfirmationModalVisible] =
    useState(false);

  const onIngestActionConfirmation = async (context: LockActionContext) => {
    switch (context.action) {
      case LockAction.AcquireRawDataResourceLocks: {
        const formValues = context.formValues as AcquireLockFormValues;
        await acquireResourceLocksForStateAndInstance(
          stateCode,
          rawDataInstance,
          `${formValues.name}: ${formValues.description}`,
          null
        );
        onActionConfirmed();
        break;
      }
      case LockAction.ReleaseRawDataResourceLocks: {
        await releaseResourceLocksForStateById(
          stateCode,
          rawDataInstance,
          currentLocks
            .filter((lock) => !lock.released) // only release locks that have not yet been released
            .map((lock) => lock.lockId)
        );
        onActionConfirmed();
        break;
      }
      default:
        throw new Error("Unrecognized lock action");
    }
    setIsConfirmationModalVisible(false);
  };

  const acquireLockForm = (
    <>
      <b> {action} </b>
      for
      <b> {stateCode.toUpperCase()} </b>
      in
      <b> {rawDataInstance}</b>?
      <Form form={form} layout="vertical" name="form_in_modal">
        <Form.Item
          label="Name:"
          name="name"
          rules={[
            {
              required: true,
              message:
                "Please input the person who is acquiring the resource lock",
            },
          ]}
        >
          <Input placeholder="i.e. genevieve heidkamp" />
        </Form.Item>
        <Form.Item
          label="Reason:"
          name="description"
          rules={[
            {
              required: true,
              message: "Please input the lock reason",
            },
          ]}
        >
          <Input placeholder="i.e. grabbing lock for some non-anna approved maneuver...." />
        </Form.Item>
      </Form>
    </>
  );
  const releaseLockForm = (
    <>
      <b> {action} </b>
      for
      <b> {stateCode.toUpperCase()} </b>
      in
      <b> {rawDataInstance}</b>?
      <Form form={form} layout="vertical" name="form_in_modal">
        <p>
          Type <b>RELEASE</b> below to confirm releasing the following locks:
          <br />
        </p>
        <ul>
          {currentLocks
            .filter((lock) => !lock.released)
            .map((lock) => (
              <li>
                {lock.resource} held by {lock.actor}
              </li>
            ))}
        </ul>

        <p>
          <em>
            {lockState === ResourceLockState.MIXED ||
            lockState === ResourceLockState.PROCESS_HELD
              ? "CAUTION: There are some locks that are held by a platform process. If a platform process has sole read/write access to the listed raw data resources, releasing them may have unintended side effects. Please only proceed if you are sure that the process has completed without releasing locks successfully."
              : ""}
          </em>
        </p>
        <Form.Item
          name="validation"
          rules={[
            {
              required: true,
              message: "Please type RELEASE",
              pattern: /RELEASE/,
            },
          ]}
        >
          <Input placeholder="RELEASE" />
        </Form.Item>
      </Form>
    </>
  );

  return (
    <>
      <Button
        onClick={() => {
          setIsConfirmationModalVisible(true);
        }}
        style={style}
        disabled={disabled}
        key={action}
      >
        {buttonText}
      </Button>
      <Modal
        open={isConfirmationModalVisible}
        title={action}
        okText={action}
        cancelText="Cancel"
        onCancel={() => setIsConfirmationModalVisible(false)}
        onOk={() => {
          form
            .validateFields()
            .then((formValues) => {
              form.resetFields();
              onIngestActionConfirmation({
                action,
                formValues,
              });
            })
            .catch((info) => {
              form.resetFields();
            });
        }}
      >
        {action === LockAction.ReleaseRawDataResourceLocks
          ? releaseLockForm
          : acquireLockForm}
      </Modal>
    </>
  );
};

export default LockActionButton;
