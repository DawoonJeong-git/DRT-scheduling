import React, { useEffect, useState } from "react";

export default function ThemeControls() {
  const [opacity, setOpacity] = useState(0.55);

  useEffect(() => {
    document.documentElement.style.setProperty("--bar-opacity", String(opacity));
  }, [opacity]);

  return (
    <div className="theme-controls">
      <label className="slider-row">
        <span className="muted">Bar opacity</span>
        <input
          type="range"
          min="0.15"
          max="0.85"
          step="0.05"
          value={opacity}
          onChange={(e) => setOpacity(parseFloat(e.target.value))}
        />
        <span className="mono">{opacity.toFixed(2)}</span>
      </label>
    </div>
  );
}