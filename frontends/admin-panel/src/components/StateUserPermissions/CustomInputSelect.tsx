// Recidiviz - a data platform for criminal justice reform
// Copyright (C) 2024 Recidiviz, Inc.
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

import { PlusOutlined } from "@ant-design/icons";
import { Button, Divider, Input, Space } from "antd";
import { useState } from "react";

export const CustomInputSelect = ({
  menu,
  items,
  setItems,
  field,
  setField,
}: {
  menu: object;
  items: string[];
  setItems: React.Dispatch<React.SetStateAction<string[]>>;
  field: string;
  setField: (name: string, value: string) => void;
}): JSX.Element => {
  const [newItem, setNewItem] = useState("");

  const onNameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setNewItem(e.target.value);
  };

  const handleKeyUp = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      addItem();
    }
  };

  const addItem = () => {
    if (!items.includes(newItem)) {
      setItems([...items, newItem]);
    }
    setField(field, newItem);
    setNewItem("");
  };

  return (
    <>
      {menu}
      <Divider style={{ margin: "8px 0" }} />
      <Space style={{ padding: "0 8px 4px" }}>
        <Input
          placeholder="New item"
          value={newItem}
          onChange={onNameChange}
          onKeyUp={handleKeyUp}
        />
        <Button type="text" icon={<PlusOutlined />} onClick={addItem}>
          Add
        </Button>
      </Space>
    </>
  );
};

export default CustomInputSelect;
