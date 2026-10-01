import { useState } from "react";
import { getApiKey, setApiKey } from "../lib/api";

/** Prompt for the X-API-Key used by destructive endpoints (contract section 13). */
export function ApiKeyPrompt({ onClose }: { onClose: () => void }) {
  const [value, setValue] = useState(getApiKey());

  const save = () => {
    setApiKey(value.trim());
    onClose();
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3>API key</h3>
        <p className="muted">
          Destructive actions (cancel, DLQ requeue/discard/purge, queue pause/resume)
          require the X-API-Key header when the API is configured with DTQ_API_KEY.
          The key is stored only in this browser's local storage.
        </p>
        <input
          type="password"
          value={value}
          onChange={(e) => setValue(e.target.value)}
          placeholder="Enter API key (empty to clear)"
          autoFocus
        />
        <div className="row">
          <button className="btn primary" onClick={save}>
            Save
          </button>
          <button className="btn" onClick={onClose}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

export function Confirm({
  title,
  body,
  confirmLabel,
  onConfirm,
  onCancel,
}: {
  title: string;
  body: string;
  confirmLabel: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <div className="modal-backdrop" onClick={onCancel}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h3>{title}</h3>
        <p className="muted">{body}</p>
        <div className="row">
          <button className="btn danger" onClick={onConfirm}>
            {confirmLabel}
          </button>
          <button className="btn" onClick={onCancel}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}
