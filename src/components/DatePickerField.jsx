import React from "react";

export default function DatePickerField({ value, onChange }) {
  return (
    <label className="date-field">
      <span className="label">Date</span>
      <input
        className="input"
        type="date"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
    </label>
  );
}