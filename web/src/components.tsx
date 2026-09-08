import {
  cloneElement,
  isValidElement,
  useEffect,
  useId,
  useRef,
  useState,
  type ReactElement,
  type ReactNode,
  type FormEvent,
} from "react";
import {
  ArrowsClockwise,
  ArrowRight,
  ArrowUp,
  Check,
  CheckCircle,
  CircleNotch,
  Copy,
  Desktop,
  Folder,
  FolderOpen,
  HardDrives,
  Info,
  Pause,
  Play,
  WarningCircle,
  X,
} from "@phosphor-icons/react";
import type {
  DeviceInput,
  DeviceView,
  Directory,
  FolderInput,
  FolderView,
} from "./types";
import { errorMessage } from "./format";

export function Brand() {
  return (
    <div className="brand">
      <span className="brand-symbol" aria-hidden="true">
        <svg viewBox="0 0 36 36" fill="none">
          <path
            d="M4 23.5 14.1 6h8.1L12.1 23.5H4Z"
            fill="currentColor"
          />
          <path
            d="m13.8 30 10.1-17.5H32L21.9 30h-8.1Z"
            fill="currentColor"
          />
          <path
            d="m15.7 17.3 4.1-7.1 6.6 11.5-4.1 7.1-6.6-11.5Z"
            fill="currentColor"
            opacity=".45"
          />
        </svg>
      </span>
      <span className="brand-wordmark">DeltaWeave</span>
    </div>
  );
}
export function ErrorBox({ error }: { error: string }) {
  return error ? (
    <div className="form-error" role="alert">
      {error}
    </div>
  ) : null;
}
export function Empty({
  title,
  description,
  action,
  compact = false,
  icon = <FolderOpen size={26} />,
}: {
  title: string;
  description: string;
  action?: ReactNode;
  compact?: boolean;
  icon?: ReactNode;
}) {
  return (
    <div className={`empty ${compact ? "compact" : ""}`}>
      <div className="empty-icon">{icon}</div>
      <h3>{title}</h3>
      <p>{description}</p>
      {action}
    </div>
  );
}
export const statusLabels: Record<FolderView["status"], string> = {
  starting: "시작 중",
  idle: "주기 대기",
  syncing: "동기화 중",
  pausing: "일시정지 대기",
  paused: "일시정지",
  listening: "수신 대기",
  error: "확인 필요",
  stopped: "중지됨",
};
export function Status({ status }: { status: FolderView["status"] }) {
  return (
    <span className={`status ${status}`}>
      <span className="dot" />
      {statusLabels[status]}
    </span>
  );
}

export function Login({
  onLogin,
  initialError = "",
}: {
  onLogin: (token: string) => Promise<unknown>;
  initialError?: string;
}) {
  const [token, setToken] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState(initialError);
  const inputId = useId();
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (pending) return;
    setPending(true);
    setError("");
    try {
      await onLogin(token.trim());
      setToken("");
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setPending(false);
    }
  }
  return (
    <div className="login-page">
      <header className="login-masthead">
        <Brand />
        <span className="login-context">
          <FolderOpen size={17} weight="duotone" aria-hidden="true" />
          파일 동기화 워크스페이스
        </span>
      </header>
      <main className="login-layout">
        <section className="login-story" aria-labelledby="login-story-title">
          <div className="login-message">
            <p className="login-kicker">Your files. In good company.</p>
            <h1 id="login-story-title">
              흩어진 파일,
              <br />
              <span>하나의 흐름.</span>
            </h1>
            <p>
              PC와 NAS, 서버의 동기화 폴더를
              <br />
              한곳에서 살펴보고 관리하세요.
            </p>
          </div>
          <div className="login-illustration" aria-hidden="true">
            <div className="weave-orbit" />
            <div className="weave-connector one" />
            <div className="weave-connector two" />
            <div className="weave-folder back">
              <div className="weave-folder-tab" />
              <div className="weave-folder-body">
                <Folder size={26} weight="duotone" />
              </div>
            </div>
            <div className="weave-folder front">
              <div className="weave-folder-tab" />
              <div className="weave-document">
                <span />
                <span />
                <span />
              </div>
              <div className="weave-folder-body">
                <ArrowsClockwise size={30} weight="bold" />
                <span>In sync.</span>
              </div>
            </div>
            <div className="weave-device computer">
              <Desktop size={27} weight="duotone" />
            </div>
            <div className="weave-device storage">
              <HardDrives size={27} weight="duotone" />
            </div>
            <span className="illustration-caption">PC · NAS · Server</span>
          </div>
        </section>
        <section className="login-form-side" aria-labelledby="login-title">
          <form className="login-form" onSubmit={submit}>
            <div className="login-form-heading">
              <span className="login-form-index">My workspace</span>
              <h2 id="login-title">내 콘솔에 로그인</h2>
              <p>이 서버의 관리자 접근 키로 시작하세요.</p>
            </div>
            <div className="field">
              <label className="field-label" htmlFor={inputId}>
                관리자 접근 키
              </label>
              <input
                id={inputId}
                type="password"
                autoComplete="current-password"
                placeholder="관리자 접근 키 입력"
                value={token}
                onChange={(e) => setToken(e.target.value)}
                required
                autoFocus
                disabled={pending}
              />
            </div>
            <ErrorBox error={error} />
            <button className="btn primary" type="submit" disabled={pending}>
              {pending ? <CircleNotch className="spinner" size={17} /> : null}
              {pending ? "로그인 중…" : "콘솔에 로그인"}
              {!pending && <ArrowRight size={18} weight="bold" />}
            </button>
            <details className="login-help">
              <summary>
                <Info size={16} aria-hidden="true" />
                접근 키 안내
              </summary>
              <p>
                서버를 시작할 때 안내된 관리자 접근 키를 사용하세요.
                장치 연결에 쓰는 공개 ID와는 다른 값입니다.
              </p>
            </details>
          </form>
        </section>
      </main>
      <footer className="login-foot">
        <span>DeltaWeave</span>
        <span>내 장치에서 실행되는 동기화 콘솔</span>
      </footer>
    </div>
  );
}

export function Modal({
  title,
  subtitle,
  children,
  onClose,
  drawer = false,
  wide = false,
}: {
  title: string;
  subtitle?: string;
  children: ReactNode;
  onClose: () => void;
  drawer?: boolean;
  wide?: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  const titleId = useId();
  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const oldOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const el = ref.current!;
    const focusables = () =>
      Array.from(
        el.querySelectorAll<HTMLElement>(
          'button:not(:disabled),input:not(:disabled),select:not(:disabled),textarea:not(:disabled),a[href],[tabindex="0"]',
        ),
      ).filter((x) => !x.closest("details:not([open])") && !x.hidden);
    (
      focusables().find((x) => ["INPUT", "TEXTAREA", "SELECT"].includes(x.tagName)) ??
      focusables()[0] ??
      el
    ).focus();
    function key(e: KeyboardEvent) {
      if (e.key !== "Escape" && e.key !== "Tab") return;
      const dialogs = document.querySelectorAll<HTMLElement>(
        '[role="dialog"][aria-modal="true"]',
      );
      if (!el.isConnected || dialogs[dialogs.length - 1] !== el) return;
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        closeRef.current();
      }
      if (e.key === "Tab") {
        const items = focusables();
        const first = items[0];
        const last = items[items.length - 1];
        if (!first) {
          e.preventDefault();
          el.focus();
        } else if (!el.contains(document.activeElement)) {
          e.preventDefault();
          (e.shiftKey ? last : first).focus();
        } else if (
          e.shiftKey &&
          (document.activeElement === first || document.activeElement === el)
        ) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    }
    document.addEventListener("keydown", key);
    return () => {
      document.removeEventListener("keydown", key);
      document.body.style.overflow = oldOverflow;
      // React may finish removing the dialog after this passive-effect cleanup.
      // Restore focus after the overlay is detached so the browser cannot move
      // it back to BODY while the overlay is being removed.
      if (previous?.isConnected) {
        const restoreFocus = () => {
          // StrictMode's effect probe cleans up while this node is still
          // connected. A real unmount leaves no dialog to receive focus; if a
          // replacement dialog is already open, let that dialog keep focus.
          const dialogs = document.querySelectorAll<HTMLElement>(
            '[role="dialog"][aria-modal="true"]',
          );
          const topmost = dialogs[dialogs.length - 1];
          if (
            !el.isConnected &&
            previous.isConnected &&
            (dialogs.length === 0 || topmost?.contains(previous))
          ) {
            previous.focus({ preventScroll: true });
          }
        };
        queueMicrotask(() => {
          if (el.isConnected) window.setTimeout(restoreFocus, 0);
          else restoreFocus();
        });
      }
    };
  }, []);
  return (
    <div
      className={`modal-backdrop ${drawer ? "drawer-backdrop" : ""}`}
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className={`modal ${drawer ? "drawer" : ""} ${wide ? "wide" : ""}`}
        ref={ref}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={subtitle ? `${titleId}-description` : undefined}
        tabIndex={-1}
      >
        <div className="modal-head">
          <div>
            <h2 id={titleId}>{title}</h2>
            {subtitle && <p id={`${titleId}-description`}>{subtitle}</p>}
          </div>
          <button className="icon-btn" onClick={onClose} aria-label="닫기">
            <X size={20} />
          </button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}

export function FolderControls({
  folder,
  onCommand,
  disabled = false,
}: {
  folder: FolderView;
  onCommand: (command: "sync" | "pause" | "resume") => Promise<unknown>;
  disabled?: boolean;
}) {
  const [syncPending, setSyncPending] = useState(false);
  const [controlPending, setControlPending] = useState<
    "pause" | "resume" | null
  >(null);
  const [error, setError] = useState("");
  async function command(value: "sync" | "pause" | "resume") {
    if (disabled || controlPending || (value === "sync" && syncPending)) return;
    if (value === "sync") setSyncPending(true);
    else setControlPending(value);
    setError("");
    try {
      await onCommand(value);
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      if (value === "sync") setSyncPending(false);
      else setControlPending(null);
    }
  }
  const blocked = controlPending !== null || disabled;
  const paused = folder.status === "paused" || folder.status === "stopped";
  return (
    <div className="folder-actions">
      {folder.role === "sync" && (
        <button
          className="icon-btn"
          aria-label={`${folder.name} 지금 동기화`}
          title="지금 동기화"
          disabled={
            syncPending || blocked || !["idle", "error"].includes(folder.status)
          }
          onClick={() => command("sync")}
        >
          <ArrowsClockwise
            size={17}
            className={folder.status === "syncing" ? "spinner" : ""}
          />
        </button>
      )}
      {folder.status === "pausing" ? (
        <span className="controls-note">현재 작업 종료 후 정지</span>
      ) : (
        <button
          className="icon-btn"
          title={paused ? "재개" : "일시정지"}
          aria-label={`${folder.name} ${paused ? "재개" : "일시정지"}`}
          disabled={blocked || folder.status === "starting"}
          onClick={() => command(paused ? "resume" : "pause")}
        >
          {paused ? <Play size={16} /> : <Pause size={16} />}
        </button>
      )}
      {controlPending === "pause" && folder.status !== "pausing" && (
        <span className="controls-note">일시정지 요청 중…</span>
      )}
      {(syncPending || controlPending) && (
        <CircleNotch className="spinner" size={12} aria-label="요청 중" />
      )}
      {error && (
        <span className="inline-error" role="alert">
          {error}
        </span>
      )}
    </div>
  );
}

export function Field({
  label,
  help,
  children,
}: {
  label: string;
  help?: string;
  children: ReactNode;
}) {
  const id = useId();
  return (
    <div className="field">
      <label className="field-label" htmlFor={id}>
        {label}
      </label>
      {isValidElement(children)
        ? cloneElement(
            children as ReactElement<{
              id?: string;
              "aria-describedby"?: string;
            }>,
            { id, "aria-describedby": help ? `${id}-help` : undefined },
          )
        : children}
      {help && <small id={`${id}-help`}>{help}</small>}
    </div>
  );
}
const lines = (value: string) =>
  value
    .split(/[\n,]+/)
    .map((v) => v.trim())
    .filter(Boolean);
export function publicConnection(value: {
  name: string;
  endpoint_id: string;
  addresses?: string[];
  address?: string;
}) {
  return JSON.stringify(
    {
      protocol_version: 1,
      name: value.name,
      endpoint_id: value.endpoint_id,
      addresses: value.addresses ?? (value.address ? [value.address] : []),
    },
    null,
    2,
  );
}
function parseConnection(text: string): {
  name: string;
  endpoint_id: string;
  addresses: string[];
} {
  const value = JSON.parse(text);
  if (
    !value ||
    typeof value.endpoint_id !== "string" ||
    !value.endpoint_id.trim()
  )
    throw new Error("공개 endpoint_id가 있는 연결 정보를 입력하세요.");
  if (value.protocol_version !== undefined && value.protocol_version !== 1)
    throw new Error("지원하지 않는 연결 정보 버전입니다.");
  const addresses = value.addresses ?? (value.address ? [value.address] : []);
  if (
    !Array.isArray(addresses) ||
    !addresses.every((v: unknown) => typeof v === "string")
  )
    throw new Error("주소는 문자열 목록이어야 합니다.");
  return {
    name: typeof value.name === "string" ? value.name : "",
    endpoint_id: value.endpoint_id.trim(),
    addresses,
  };
}

export function FolderForm({
  folder,
  devices,
  onSubmit,
  onSaved,
  browse,
  defaultInterval = 30,
}: {
  folder?: FolderView;
  devices: DeviceView[];
  onSubmit: (value: FolderInput) => Promise<unknown>;
  onSaved: () => void;
  browse: (path: string) => Promise<Directory>;
  defaultInterval?: number;
}) {
  const [form, setForm] = useState<FolderInput>(() =>
    folder
      ? { ...folder }
      : {
          name: "",
          root: "",
          role: "sync",
          interval_seconds: defaultInterval,
          max_connections: 8,
          min_free_space_mib: 128,
          enabled: true,
          bind: "0.0.0.0:0",
        },
  );
  const [addresses, setAddresses] = useState(
    folder?.direct_addresses?.join("\n") ?? "",
  );
  const [peers, setPeers] = useState(folder?.allowed_peers?.join("\n") ?? "");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const [directory, setDirectory] = useState<Directory | null>(null);
  const [browsing, setBrowsing] = useState(false);
  const [browseError, setBrowseError] = useState("");
  const [importText, setImportText] = useState("");
  const [importMode, setImportMode] = useState<"connection" | "config">(
    "connection",
  );
  function update<K extends keyof FolderInput>(key: K, value: FolderInput[K]) {
    setForm((previous) => ({ ...previous, [key]: value }));
  }
  async function browsePath(path: string) {
    setBrowsing(true);
    setBrowseError("");
    try {
      setDirectory(await browse(path));
    } catch (e) {
      setBrowseError(errorMessage(e));
    } finally {
      setBrowsing(false);
    }
  }
  function importConfig() {
    setError("");
    try {
      if (importMode === "connection") {
        const data = parseConnection(importText);
        update("peer_endpoint_id", data.endpoint_id);
        setAddresses(data.addresses.join("\n"));
      } else {
        const data = JSON.parse(importText) as Record<string, unknown>;
        if (
          typeof data.root !== "string" ||
          !["sync", "receive"].includes(String(data.role))
        )
          throw new Error(
            "root 경로와 sync 또는 receive 역할을 포함한 설정을 입력하세요.",
          );
        const keys = [
          "name",
          "root",
          "role",
          "state_path",
          "identity_path",
          "device_id",
          "peer_endpoint_id",
          "bind",
          "enabled",
          "interval_seconds",
          "max_connections",
          "min_free_space_mib",
        ] as const;
        const imported: Record<string, unknown> = {};
        for (const key of keys) {
          const value = data[key];
          if (value === undefined || value === null) continue;
          if (
            [
              "interval_seconds",
              "max_connections",
              "min_free_space_mib",
            ].includes(key)
          ) {
            if (
              typeof value !== "number" ||
              !Number.isSafeInteger(value) ||
              value < 0
            )
              throw new Error(`${key} 값은 0 이상의 정수여야 합니다.`);
          } else if (key === "enabled") {
            if (typeof value !== "boolean")
              throw new Error("enabled 값은 true 또는 false여야 합니다.");
          } else if (typeof value !== "string")
            throw new Error(`${key} 값은 문자열이어야 합니다.`);
          imported[key] = value;
        }
        for (const key of ["direct_addresses", "allowed_peers"]) {
          if (
            data[key] !== undefined &&
            (!Array.isArray(data[key]) ||
              !(data[key] as unknown[]).every(
                (value) => typeof value === "string",
              ))
          )
            throw new Error(`${key} 값은 문자열 목록이어야 합니다.`);
        }
        if (folder && imported.role !== folder.role)
          throw new Error("기존 폴더의 역할은 변경할 수 없습니다.");
        setForm((previous) => ({ ...previous, ...imported }));
        if (
          Array.isArray(data.direct_addresses) &&
          data.direct_addresses.every((x) => typeof x === "string")
        )
          setAddresses(data.direct_addresses.join("\n"));
        if (
          Array.isArray(data.allowed_peers) &&
          data.allowed_peers.every((x) => typeof x === "string")
        )
          setPeers(data.allowed_peers.join("\n"));
      }
      setImportText("");
    } catch (e) {
      setError(errorMessage(e));
    }
  }
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (pending) return;
    setPending(true);
    setError("");
    const input: FolderInput = {
      name: form.name.trim(),
      root: form.root.trim(),
      role: form.role,
      enabled: form.enabled,
      interval_seconds: form.interval_seconds,
      max_connections: form.max_connections,
      min_free_space_mib: form.min_free_space_mib,
      direct_addresses: lines(addresses),
      allowed_peers: lines(peers),
    };
    for (const key of [
      "state_path",
      "identity_path",
      "device_id",
      "peer_endpoint_id",
      "bind",
    ] as const)
      if (form[key]?.trim()) input[key] = form[key]!.trim();
    try {
      await onSubmit(input);
      onSaved();
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setPending(false);
    }
  }
  return (
    <form onSubmit={submit}>
      <div className="notice">
        <Info size={17} />
        <span>
          폴더 경로는 <strong>현재 관리 서버가 실행되는 장치</strong>의
          경로입니다. 기존 CLI가 같은 폴더를 사용 중이면 먼저 해당 작업을
          종료하세요.
        </span>
      </div>
      <fieldset disabled={pending} className="form-fields">
        <Field label="폴더 이름">
          <input
            value={form.name}
            onChange={(e) => update("name", e.target.value)}
            placeholder="예: 작업 문서"
            required
          />
        </Field>
        <div className="field">
          <label className="field-label" htmlFor="folder-root">
            로컬 폴더 경로
          </label>
          <div className="input-action">
            <input
              id="folder-root"
              value={form.root}
              onChange={(e) => update("root", e.target.value)}
              placeholder="/volume1/documents 또는 C:\Documents"
              required
            />
            <button
              type="button"
              className="btn subtle"
              disabled={browsing}
              onClick={() => browsePath(form.root)}
            >
              <FolderOpen size={16} />
              찾아보기
            </button>
          </div>
          <small>동기화 파일이 저장되는 전체 경로를 입력하세요.</small>
          {browsing && <p className="muted">폴더를 불러오는 중…</p>}
          {browseError && <ErrorBox error={browseError} />}
          {directory && (
            <div className="browse-box">
              <div className="browse-path">
                <code>{directory.path}</code>
                <button
                  type="button"
                  className="text-button"
                  onClick={() => {
                    update("root", directory.path);
                    setDirectory(null);
                  }}
                >
                  이 폴더 선택
                </button>
                <button
                  type="button"
                  className="icon-btn"
                  aria-label="폴더 탐색 닫기"
                  onClick={() => setDirectory(null)}
                >
                  <X size={15} />
                </button>
              </div>
              <div className="directory-list">
                {directory.parent && (
                  <button
                    type="button"
                    disabled={browsing}
                    onClick={() => browsePath(directory.parent!)}
                  >
                    <ArrowUp size={15} />
                    상위 폴더
                  </button>
                )}
                {directory.entries.map((entry) => (
                  <button
                    type="button"
                    key={entry.path}
                    disabled={browsing}
                    onClick={() => browsePath(entry.path)}
                  >
                    <Folder size={16} />
                    {entry.name}
                    <ArrowRight size={13} />
                  </button>
                ))}
                {!directory.entries.length && (
                  <p className="muted directory-empty">하위 폴더가 없습니다.</p>
                )}
              </div>
            </div>
          )}
        </div>
        <div className="form-grid">
          <Field
            label="연결 역할"
            help="두 역할 모두 양방향 변경을 주고받습니다."
          >
            <select
              value={form.role}
              disabled={!!folder}
              onChange={(e) =>
                update("role", e.target.value as FolderInput["role"])
              }
            >
              <option value="sync">상대 폴더에 연결</option>
              <option value="receive">연결 요청 수신</option>
            </select>
          </Field>
          <Field
            label="연결 장치"
            help="등록한 장치를 선택하거나 연결 정보를 직접 입력하세요."
          >
            <select
              value={form.device_id ?? ""}
              onChange={(e) => {
                const device = devices.find((d) => d.id === e.target.value);
                update("device_id", e.target.value);
                if (device) {
                  update("peer_endpoint_id", device.endpoint_id);
                  setAddresses(device.address);
                }
              }}
            >
              <option value="">직접 입력</option>
              {devices.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.name}
                </option>
              ))}
            </select>
          </Field>
        </div>
        {form.role === "sync" ? (
          <>
            <Field label="상대 장치 공개 ID">
              <input
                className="mono"
                value={form.peer_endpoint_id ?? ""}
                onChange={(e) => update("peer_endpoint_id", e.target.value)}
                required
                placeholder="상대 폴더의 endpoint ID"
              />
            </Field>
            <Field
              label="상대 장치 주소"
              help="주소를 한 줄에 하나씩 입력하세요. 예: 192.168.1.20:9000"
            >
              <textarea
                className="mono"
                value={addresses}
                onChange={(e) => setAddresses(e.target.value)}
                placeholder="IP:포트"
              />
            </Field>
          </>
        ) : (
          <Field
            label="허용할 장치 공개 ID"
            help="접근을 허용할 상대 endpoint ID를 한 줄에 하나씩 입력하세요."
          >
            <textarea
              className="mono"
              value={peers}
              onChange={(e) => setPeers(e.target.value)}
              placeholder="PC에서 복사한 공개 endpoint ID"
            />
          </Field>
        )}
        <details className="advanced">
          <summary>기존 설정 · 공개 연결 정보 가져오기</summary>
          <div className="form-fields">
            <Field label="가져올 정보">
              <select
                value={importMode}
                onChange={(e) =>
                  setImportMode(e.target.value as "connection" | "config")
                }
              >
                <option value="connection">상대 장치의 공개 연결 정보</option>
                <option value="config">기존 폴더 설정 JSON</option>
              </select>
            </Field>
            <Field
              label="설정 JSON"
              help="기존 설정은 root, role, state_path, identity_path와 연결 값을 지원합니다. 키 파일의 내용은 붙여 넣지 마세요."
            >
              <textarea
                className="mono"
                value={importText}
                onChange={(e) => setImportText(e.target.value)}
              />
            </Field>
            <button
              type="button"
              className="btn subtle"
              disabled={!importText.trim()}
              onClick={importConfig}
            >
              폼에 가져오기
            </button>
          </div>
        </details>
        <details className="advanced">
          <summary>고급 설정</summary>
          <div className="form-fields">
            <div className="form-grid">
              <Field label="동기화 간격 (초)">
                <input
                  type="number"
                  min="1"
                  max="86400"
                  required
                  value={form.interval_seconds ?? 30}
                  onChange={(e) =>
                    update("interval_seconds", Number(e.target.value))
                  }
                />
              </Field>
              <Field label="수신 연결 수">
                <input
                  type="number"
                  min="1"
                  max="1024"
                  required
                  value={form.max_connections ?? 8}
                  onChange={(e) =>
                    update("max_connections", Number(e.target.value))
                  }
                />
              </Field>
            </div>
            <div className="form-grid">
              <Field label="최소 여유 공간 (MiB)">
                <input
                  type="number"
                  min="0"
                  required
                  value={form.min_free_space_mib ?? 128}
                  onChange={(e) =>
                    update("min_free_space_mib", Number(e.target.value))
                  }
                />
              </Field>
              <Field label="로컬 수신 주소">
                <input
                  className="mono"
                  value={form.bind ?? ""}
                  onChange={(e) => update("bind", e.target.value)}
                  placeholder="0.0.0.0:0"
                />
              </Field>
            </div>
            <Field
              label="상태 저장 경로"
              help="비워 두면 전용 경로를 자동으로 만듭니다."
            >
              <input
                value={form.state_path ?? ""}
                onChange={(e) => update("state_path", e.target.value)}
              />
            </Field>
            <Field
              label="기존 identity 파일 경로"
              help="기존 공개 ID를 유지하려면 사용하던 파일 경로를 입력하세요."
            >
              <input
                value={form.identity_path ?? ""}
                onChange={(e) => update("identity_path", e.target.value)}
              />
            </Field>
          </div>
        </details>
        <label className="checkbox-label">
          <input
            type="checkbox"
            checked={form.enabled ?? true}
            onChange={(e) => update("enabled", e.target.checked)}
          />
          저장 후 폴더 작업 활성화
        </label>
      </fieldset>
      <ErrorBox error={error} />
      <div className="form-actions">
        <span className="muted">서버에서 경로와 연결 설정을 확인합니다.</span>
        <button type="submit" className="btn primary" disabled={pending}>
          {pending ? (
            <CircleNotch className="spinner" size={16} />
          ) : (
            <Check size={16} />
          )}
          {pending ? "저장 중…" : folder ? "변경 사항 저장" : "폴더 추가"}
        </button>
      </div>
    </form>
  );
}

export function DeviceForm({
  device,
  onSubmit,
  onSaved,
}: {
  device?: DeviceView;
  onSubmit: (value: DeviceInput) => Promise<unknown>;
  onSaved: () => void;
}) {
  const [form, setForm] = useState<DeviceInput>(
    device
      ? {
          name: device.name,
          endpoint_id: device.endpoint_id,
          address: device.address,
        }
      : { name: "", endpoint_id: "", address: "" },
  );
  const [json, setJson] = useState("");
  const [error, setError] = useState("");
  const [pending, setPending] = useState(false);
  async function submit(e: FormEvent) {
    e.preventDefault();
    setPending(true);
    setError("");
    try {
      await onSubmit({
        name: form.name.trim(),
        endpoint_id: form.endpoint_id.trim(),
        address: form.address.trim(),
      });
      onSaved();
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setPending(false);
    }
  }
  function importInfo() {
    try {
      const value = parseConnection(json);
      setForm({
        name: value.name || form.name,
        endpoint_id: value.endpoint_id,
        address: value.addresses[0] ?? "",
      });
      setJson("");
      setError("");
    } catch (e) {
      setError(errorMessage(e));
    }
  }
  return (
    <form onSubmit={submit}>
      <div className="notice">
        <Info size={17} />
        <span>
          장치를 등록한 뒤 폴더의 연결 설정에서 선택하세요. 등록만으로
          연결되거나 접근이 허용되지는 않습니다.
        </span>
      </div>
      <fieldset disabled={pending} className="form-fields">
        <Field label="장치 이름">
          <input
            value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })}
            placeholder="예: 집 NAS"
            required
          />
        </Field>
        <Field label="공개 endpoint ID">
          <input
            className="mono"
            value={form.endpoint_id}
            onChange={(e) => setForm({ ...form, endpoint_id: e.target.value })}
            required
          />
        </Field>
        <Field label="장치 주소" help="IP:포트 형식의 직접 연결 주소">
          <input
            className="mono"
            value={form.address}
            onChange={(e) => setForm({ ...form, address: e.target.value })}
            placeholder="192.168.1.20:9000"
            required
          />
        </Field>
        <details className="advanced">
          <summary>공개 연결 정보 붙여 넣기</summary>
          <Field label="공개 연결 정보 JSON">
            <textarea
              className="mono"
              value={json}
              onChange={(e) => setJson(e.target.value)}
            />
          </Field>
          <button
            className="btn subtle small"
            type="button"
            disabled={!json.trim()}
            onClick={importInfo}
          >
            폼에 가져오기
          </button>
        </details>
      </fieldset>
      <ErrorBox error={error} />
      <div className="form-actions">
        <button className="btn primary" disabled={pending}>
          {pending ? "저장 중…" : device ? "변경 사항 저장" : "장치 추가"}
          <Check size={16} />
        </button>
      </div>
    </form>
  );
}

export function CopyButton({
  value,
  label = "공개 연결 정보 복사",
}: {
  value: string;
  label?: string;
}) {
  const [copied, setCopied] = useState(false);
  const [error, setError] = useState("");
  async function copy() {
    setError("");
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value);
      } else {
        const previousFocus = document.activeElement as HTMLElement | null;
        const textarea = document.createElement("textarea");
        textarea.value = value;
        textarea.style.position = "fixed";
        textarea.style.opacity = "0";
        try {
          document.body.append(textarea);
          textarea.select();
          if (!document.execCommand("copy"))
            throw new Error(
              "자동 복사가 불가능합니다. 표시된 연결 정보를 선택해 복사하세요.",
            );
        } finally {
          textarea.remove();
          previousFocus?.focus({ preventScroll: true });
        }
      }
      setCopied(true);
    } catch (e) {
      setError(errorMessage(e));
    }
  }
  return (
    <>
      <button type="button" className="btn subtle small" onClick={copy}>
        {copied ? <CheckCircle size={15} /> : <Copy size={15} />}
        {copied ? "복사됨" : label}
      </button>
      {error && <ErrorBox error={error} />}
    </>
  );
}

export function ConfirmRemove({
  name,
  kind,
  onRemove,
  onClose,
}: {
  name: string;
  kind: "folder" | "device";
  onRemove: () => Promise<unknown>;
  onClose: () => void;
}) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  async function remove() {
    setPending(true);
    setError("");
    try {
      await onRemove();
      onClose();
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setPending(false);
    }
  }
  return (
    <Modal
      title={`${kind === "folder" ? "폴더" : "장치"} 연결 제거`}
      onClose={() => {
        if (!pending) onClose();
      }}
    >
      <div className="notice warning">
        <WarningCircle size={20} />
        <span>
          <strong>{name}</strong>의 관리 설정을 제거합니다.
          {kind === "folder"
            ? " 동기화된 파일은 그대로 보존됩니다."
            : " 연결 중인 폴더가 있다면 해당 연결 설정도 확인하세요."}
        </span>
      </div>
      <ErrorBox error={error} />
      <div className="form-actions">
        <button className="btn subtle" disabled={pending} onClick={onClose}>
          취소
        </button>
        <button className="btn danger" disabled={pending} onClick={remove}>
          {pending ? "제거 중…" : "연결 제거"}
        </button>
      </div>
    </Modal>
  );
}
