import {
  ArrowRight,
  ArrowUp,
  ArrowsClockwise,
  Check,
  CheckCircle,
  CircleNotch,
  CloudArrowUp,
  Clock,
  Folder,
  FolderOpen,
  Info,
  Key,
  LinkSimple,
  LockKey,
  Pause,
  Play,
  Plus,
  ShieldCheck,
  Trash,
  UserCircle,
  UsersThree,
  X,
} from "@phosphor-icons/react";
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { Api, ApiError, OperationRequest } from "./api";
import { CopyButton, Empty, ErrorBox, Field } from "./components";
import { ago, bytes, date, number } from "./format";
import type {
  Directory,
  IssuedKey,
  JoinResult,
  KeyPreview,
  KeySummary,
  ManagedShareView,
  ManagedStatus,
  MemberView,
  PendingView,
  Permission,
} from "./types";

export const managedStatusLabels: Record<ManagedStatus, string> = {
  waiting: "가입 대기",
  offline: "오프라인",
  initial_sync: "초기 동기화",
  complete: "동기화 완료",
  conflict: "충돌 확인",
  revoked: "권한 철회됨",
  error: "확인 필요",
  paused: "일시정지",
};

export const permissionLabels: Record<Permission, string> = {
  read_only: "읽기 전용",
  read_write: "읽기/쓰기",
};

function permissionLabel(permission: Permission | null | undefined) {
  return permission ? permissionLabels[permission] : "권한 확인 중";
}

function roleLabel(role: ManagedShareView["role"]) {
  return role === "owner" ? "소유자" : "멤버";
}

const managedErrorMessages: Record<string, string> = {
  invalid_request_id: "요청 ID 형식이 올바르지 않습니다.",
  invalid_json: "요청 형식을 읽을 수 없습니다.",
  invalid_request: "입력 내용을 확인하세요.",
  invalid_ticket: "공유 키를 확인하세요.",
  unsupported_version: "지원하지 않는 공유 키입니다.",
  expired: "공유 키가 만료되었습니다.",
  invitation_revoked: "철회된 공유 키입니다.",
  member_revoked: "이 멤버의 권한은 철회되었습니다.",
  not_member: "이 장치는 공유 멤버로 등록되지 않았습니다.",
  permission_denied: "이 작업을 수행할 권한이 없습니다.",
  owner_mismatch: "공유 소유자를 확인할 수 없습니다.",
  owner_only: "공유 소유자만 이 작업을 수행할 수 있습니다.",
  unauthorized: "관리 세션이 만료되었습니다. 다시 로그인하세요.",
  not_found: "공유를 찾을 수 없습니다.",
  unknown_share: "공유를 찾을 수 없습니다.",
  unknown_invitation: "발급 키를 찾을 수 없습니다.",
  unknown_member: "멤버를 찾을 수 없습니다.",
  invalid_id: "식별자 형식이 올바르지 않습니다.",
  invalid_member_id: "멤버 식별자 형식이 올바르지 않습니다.",
  invalid_path: "선택한 폴더를 사용할 수 없습니다.",
  path_denied: "선택한 폴더를 사용할 수 없습니다.",
  offline: "소유자에 연결할 수 없습니다. 잠시 후 다시 시도하세요.",
  private_state_unavailable: "공유 상태를 사용할 수 없습니다. 잠시 후 다시 시도하세요.",
  state_unavailable: "공유 상태를 사용할 수 없습니다. 잠시 후 다시 시도하세요.",
  shutdown: "관리 서비스가 종료 중입니다.",
  transfer_failed: "공유 전송을 완료하지 못했습니다. 다시 시도하세요.",
  busy: "공유 작업이 진행 중입니다. 잠시 후 다시 시도하세요.",
  duplicate: "이미 처리된 요청입니다.",
  idempotency_conflict: "같은 요청 ID에 다른 내용이 사용되었습니다.",
  key_response_expired: "한 번만 표시되는 키 응답이 만료되었습니다. 새 요청을 시작하세요.",
  idempotency_capacity: "요청을 잠시 처리할 수 없습니다. 다시 시도하세요.",
  pending_expired: "대기 중인 가입 요청이 만료되었습니다.",
  replica_claim_rejected: "이 장치의 기존 멤버 연결을 확인할 수 없습니다.",
  invalid_record: "공유 상태 기록을 확인할 수 없습니다.",
  protocol: "공유 연결을 확인할 수 없습니다.",
  internal_error: "공유 요청을 처리하지 못했습니다. 잠시 후 다시 시도하세요.",
};

/** Map server classifications to safe Korean copy before any fallback text. */
export function safeShareError(error: unknown, _secret = "") {
  if (error instanceof ApiError) {
    if (error.code && managedErrorMessages[error.code]) {
      return managedErrorMessages[error.code];
    }
    if (error.status === 401) return managedErrorMessages.unauthorized;
    if (error.status === 403) return managedErrorMessages.permission_denied;
    if (error.status === 404) return managedErrorMessages.not_found;
    if (error.status === 422) return managedErrorMessages.invalid_request;
    if (error.status === 503) return managedErrorMessages.offline;
    return "공유 요청을 처리하지 못했습니다. 잠시 후 다시 시도하세요.";
  }
  // Local browse failures have no safe server classification. Do not render
  // arbitrary Error text because it may contain a private path or credential.
  return "요청을 처리하지 못했습니다. 다시 시도하세요.";
}

function shortOpaqueId(value: string) {
  return value.length > 16 ? `${value.slice(0, 8)}…${value.slice(-4)}` : value;
}

function statusClass(status: ManagedStatus) {
  return `managed-status ${status}`;
}

function ShareStatus({ status }: { status: ManagedStatus }) {
  return (
    <span className={statusClass(status)}>
      <span className="dot" />
      {managedStatusLabels[status]}
    </span>
  );
}

function PermissionBadge({ permission }: { permission: Permission | null }) {
  return (
    <span className={`permission-badge ${permission ?? "unknown"}`}>
      {permission === "read_only" ? <LockKey size={13} /> : <Key size={13} />}
      {permissionLabel(permission)}
    </span>
  );
}

function formatExpiry(value: number | null | undefined) {
  return value == null ? "만료 없음" : `만료 ${date(value * 1000)}`;
}

function operationFingerprint(...values: (string | number | null | undefined)[]) {
  return values.map((value) => String(value ?? "")).join("\u001f");
}

export const MAX_EXPIRY_TIMER_MS = 2_147_000_000;

export function scheduleExpiry(expiresAt: number, onExpire: () => void) {
  let timer: number | undefined;
  const check = () => {
    const remaining = expiresAt * 1000 - Date.now();
    if (remaining <= 0) {
      onExpire();
      return;
    }
    timer = window.setTimeout(check, Math.min(remaining, MAX_EXPIRY_TIMER_MS));
  };
  timer = window.setTimeout(
    check,
    Math.min(Math.max(0, expiresAt * 1000 - Date.now()), MAX_EXPIRY_TIMER_MS),
  );
  return () => {
    if (timer !== undefined) window.clearTimeout(timer);
  };
}

export function ShareDirectoryPicker({
  value,
  onChange,
  browse,
  label,
  help,
  disabled = false,
}: {
  value: string;
  onChange: (value: string) => void;
  browse: (path: string) => Promise<Directory>;
  label: string;
  help: string;
  disabled?: boolean;
}) {
  const [directory, setDirectory] = useState<Directory | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const generation = useRef(0);
  useEffect(
    () => () => {
      generation.current += 1;
    },
    [],
  );
  async function open(path: string) {
    if (disabled) return;
    const requestGeneration = ++generation.current;
    setLoading(true);
    setError("");
    try {
      const result = await browse(path.trim());
      if (requestGeneration !== generation.current) return;
      setDirectory(result);
    } catch (cause) {
      if (requestGeneration === generation.current) setError(safeShareError(cause));
    } finally {
      if (requestGeneration === generation.current) setLoading(false);
    }
  }
  function closeDirectory() {
    generation.current += 1;
    setLoading(false);
    setDirectory(null);
  }
  function chooseDirectory(path: string) {
    generation.current += 1;
    setLoading(false);
    onChange(path);
    setDirectory(null);
  }
  return (
    <div className="share-picker">
      <Field label={label} help={help}>
        <div className="input-action">
          <input
            value={value}
            onChange={(event) => onChange(event.target.value)}
            placeholder="폴더를 찾아 선택하세요"
            disabled={disabled}
            required
          />
          <button
            type="button"
            className="btn subtle"
            onClick={() => open(value)}
            disabled={disabled || loading}
          >
            {loading ? <CircleNotch className="spinner" size={16} /> : <FolderOpen size={16} />}
            찾아보기
          </button>
        </div>
      </Field>
      {error && <ErrorBox error={error} />}
      {directory && (
        <div className="browse-box share-browse-box">
          <div className="browse-path">
            <Folder size={16} />
            <code>{directory.path}</code>
            <button
              type="button"
              className="text-button"
              onClick={() => chooseDirectory(directory.path)}
              disabled={disabled}
            >
              이 폴더 선택
            </button>
            <button
              type="button"
              className="icon-btn"
              aria-label="폴더 탐색 닫기"
              onClick={closeDirectory}
            >
              <X size={15} />
            </button>
          </div>
          <div className="directory-list">
            {directory.parent && (
              <button
                type="button"
                onClick={() => open(directory.parent!)}
                disabled={disabled || loading}
              >
                <ArrowUp size={15} />
                상위 폴더
              </button>
            )}
            {directory.entries.map((entry) => (
              <button
                type="button"
                key={entry.path}
                onClick={() => open(entry.path)}
                disabled={disabled || loading}
              >
                <Folder size={16} />
                <span className="directory-entry-name">{entry.name}</span>
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
  );
}

export function ShareCreateFlow({
  api,
  browse,
  onClose,
  onComplete,
}: {
  api: Api;
  browse: (path: string) => Promise<Directory>;
  onClose: () => void;
  onComplete: () => Promise<unknown>;
}) {
  const [name, setName] = useState("");
  const [root, setRoot] = useState("");
  const [reserve, setReserve] = useState(128);
  const [share, setShare] = useState<ManagedShareView | null>(null);
  const [issued, setIssued] = useState<IssuedKey | null>(null);
  const [busy, setBusy] = useState<"create" | Permission | null>(null);
  const [error, setError] = useState("");
  const [issueError, setIssueError] = useState("");
  const createRequest = useRef(new OperationRequest());
  const issueRequests = useRef(new Map<Permission, OperationRequest>());
  const generation = useRef(0);

  useEffect(
    () => () => {
      generation.current += 1;
      createRequest.current.reset();
      issueRequests.current.forEach((request) => request.reset());
    },
    [],
  );

  useEffect(() => {
    if (issued?.expires_at == null) return;
    return scheduleExpiry(issued.expires_at, () => {
      generation.current += 1;
      setIssued(null);
      setIssueError("발급된 키가 만료되어 화면에서 지웠습니다.");
      issueRequests.current.forEach((request) => request.reset());
    });
  }, [issued]);

  const create = async (event: FormEvent) => {
    event.preventDefault();
    if (busy || share) return;
    const cleanName = name.trim();
    const cleanRoot = root.trim();
    if (!cleanName || !cleanRoot) {
      setError("공유 이름과 현재 장치의 폴더를 선택하세요.");
      return;
    }
    if (!Number.isSafeInteger(reserve) || reserve < 0) {
      setError("최소 여유 공간은 0 이상의 정수여야 합니다.");
      return;
    }
    const requestGeneration = ++generation.current;
    const fingerprint = operationFingerprint(cleanName, cleanRoot, reserve);
    const requestId = createRequest.current.begin(fingerprint);
    setBusy("create");
    setError("");
    try {
      const created = await api.createShare({
        request_id: requestId,
        name: cleanName,
        root: cleanRoot,
        min_free_space_mib: reserve,
      });
      if (requestGeneration !== generation.current) return;
      createRequest.current.reset();
      setShare(created);
      await onComplete();
    } catch (cause) {
      if (requestGeneration === generation.current)
        setError(safeShareError(cause));
    } finally {
      if (requestGeneration === generation.current) setBusy(null);
    }
  };

  async function issue(permission: Permission) {
    if (!share || busy) return;
    const requestGeneration = ++generation.current;
    const tracker = issueRequests.current.get(permission) ?? new OperationRequest();
    issueRequests.current.set(permission, tracker);
    const requestId = tracker.begin(operationFingerprint(share.share_id, permission));
    setBusy(permission);
    setIssueError("");
    try {
      const value = await api.issueKey(share.share_id, requestId, permission);
      if (requestGeneration !== generation.current) return;
      tracker.reset();
      setIssued(value);
      await onComplete();
    } catch (cause) {
      if (requestGeneration === generation.current)
        setIssueError(safeShareError(cause));
    } finally {
      if (requestGeneration === generation.current) setBusy(null);
    }
  }

  return (
    <>
      {!share ? (
        <form onSubmit={create} className="share-form">
          <div className="notice">
            <CloudArrowUp size={18} />
            <span>
              소유자가 최신 상태와 권한을 관리합니다. 이 장치의 폴더를 선택하면 다른
              기기가 검증된 청크를 받아 동기화합니다.
            </span>
          </div>
          <fieldset disabled={busy !== null} className="form-fields">
            <Field label="공유 이름" help="다른 기기에서 알아보기 쉬운 이름을 입력하세요.">
              <input
                value={name}
                onChange={(event) => setName(event.target.value)}
                placeholder="예: 팀 문서"
                autoFocus
                required
              />
            </Field>
            <ShareDirectoryPicker
              label="공유할 폴더"
              help="폴더 경로는 이 웹 콘솔을 실행하는 장치에서 선택됩니다. 브라우저의 파일을 업로드하지 않습니다."
              value={root}
              onChange={setRoot}
              browse={browse}
              disabled={busy !== null}
            />
            <Field label="최소 여유 공간 (MiB)" help="수신 기기의 저장 공간 보호에 사용됩니다.">
              <input
                type="number"
                min="0"
                step="1"
                value={reserve}
                onChange={(event) => setReserve(Number(event.target.value))}
                required
              />
            </Field>
          </fieldset>
          <ErrorBox error={error} />
          <div className="form-actions">
            <span className="muted">저장 후 읽기 전용 또는 읽기/쓰기 키를 발급합니다.</span>
            <button type="submit" className="btn primary" disabled={busy !== null}>
              {busy === "create" ? <CircleNotch className="spinner" size={16} /> : <Plus size={16} />}
              {busy === "create" ? "공유 만드는 중…" : "폴더 공유 만들기"}
            </button>
          </div>
        </form>
      ) : (
        <div className="share-issued-flow">
          <div className="share-success-banner">
            <CheckCircle size={22} weight="fill" />
            <div>
              <strong>{share.name} 공유를 만들었습니다.</strong>
              <p>키는 지금 이 화면에서 한 번만 표시됩니다. 복사한 뒤 안전하게 전달하세요.</p>
            </div>
          </div>
          <div className="share-created-summary">
            <span className="eyebrow">소유자 공유</span>
            <strong>{share.name}</strong>
            <code>{share.root}</code>
          </div>
          <div className="key-choice-grid" aria-label="공유 키 발급">
            {(["read_only", "read_write"] as Permission[]).map((permission) => (
              <button
                type="button"
                className={`key-choice ${permission}`}
                key={permission}
                onClick={() => issue(permission)}
                disabled={busy !== null}
              >
                {permission === "read_only" ? <LockKey size={22} /> : <Key size={22} />}
                <span>
                  <strong>{permissionLabel(permission)} 키</strong>
                  <small>
                    {permission === "read_only"
                      ? "읽고 검증된 조각을 공급합니다."
                      : "허용된 기기에서 변경을 주고받습니다."}
                  </small>
                </span>
                {busy === permission ? <CircleNotch className="spinner" size={16} /> : <Plus size={17} />}
              </button>
            ))}
          </div>
          {issueError && <ErrorBox error={issueError} />}
          {issued && (
            <div className="issued-key-panel" data-sensitive="share-key">
              <div className="issued-key-head">
                <div>
                  <span className="eyebrow">한 번만 표시</span>
                  <strong>{permissionLabel(issued.permission)} 키</strong>
                </div>
                <PermissionBadge permission={issued.permission} />
              </div>
              <code className="share-key-value" aria-label="발급된 공유 키">
                {issued.key}
              </code>
              <div className="copy-row">
                <CopyButton value={issued.key} label="키 복사" />
                <span className="muted">복사 실패 시 화면의 키를 직접 선택하세요.</span>
              </div>
              <p className="secret-note">
                <ShieldCheck size={15} /> 이 키는 브라우저 저장소·URL·활동 기록에 저장하지 않습니다.
              </p>
            </div>
          )}
          <div className="permission-note">
            <Info size={15} />
            <span>
              키 철회는 신규 가입만 막습니다. 이미 전달된 파일을 회수하지 않으며, 기존 멤버를
              끊으려면 멤버 관리에서 별도로 철회하세요.
            </span>
          </div>
          <div className="form-actions">
            <span className="muted">필요한 키를 복사한 뒤 공유 화면을 닫으세요.</span>
            <button type="button" className="btn primary" onClick={onClose}>
              완료
              <Check size={16} />
            </button>
          </div>
        </div>
      )}
    </>
  );
}

export function ShareJoinFlow({
  api,
  browse,
  onClose,
  onComplete,
}: {
  api: Api;
  browse: (path: string) => Promise<Directory>;
  onClose: () => void;
  onComplete: () => Promise<unknown>;
}) {
  const [key, setKey] = useState("");
  const [preview, setPreview] = useState<KeyPreview | null>(null);
  const [validated, setValidated] = useState(false);
  const [offlineValidation, setOfflineValidation] = useState(false);
  const [destination, setDestination] = useState("");
  const [result, setResult] = useState<JoinResult | null>(null);
  const [resultName, setResultName] = useState("");
  const [busy, setBusy] = useState<"check" | "join" | null>(null);
  const [joinSecretExpired, setJoinSecretExpired] = useState(false);
  const [error, setError] = useState("");
  const [joinError, setJoinError] = useState("");
  const previewRequest = useRef(new OperationRequest());
  const validateRequest = useRef(new OperationRequest());
  const joinRequest = useRef(new OperationRequest());
  const generation = useRef(0);

  useEffect(() => () => {
    generation.current += 1;
    previewRequest.current.reset();
    validateRequest.current.reset();
    joinRequest.current.reset();
  }, []);

  function changeKey(value: string) {
    // A join request owns the bearer until its response arrives. The input is
    // disabled during that mutation; keep this guard for programmatic changes.
    if (busy === "join") return;
    generation.current += 1;
    setBusy(null);
    setKey(value);
    setJoinSecretExpired(false);
    setPreview(null);
    setValidated(false);
    setOfflineValidation(false);
    setDestination("");
    setResult(null);
    setResultName("");
    setError("");
    setJoinError("");
    previewRequest.current.reset();
    validateRequest.current.reset();
    joinRequest.current.reset();
  }

  async function checkKey() {
    const clean = key.trim();
    if (!clean || busy) {
      setError("공유 키를 붙여 넣으세요.");
      return;
    }
    const requestGeneration = ++generation.current;
    const previewRequestId = previewRequest.current.begin(clean);
    const validateRequestId = validateRequest.current.begin(clean);
    setBusy("check");
    setError("");
    try {
      const value = await api.previewShareKey(previewRequestId, clean);
      if (requestGeneration !== generation.current) return;
      previewRequest.current.reset();
      setPreview(value);
      setDestination("");
      if (!value.signature_valid) {
        setValidated(false);
        setOfflineValidation(false);
        setError("공유 키 서명을 확인할 수 없습니다.");
        return;
      }
      setValidated(false);
      setOfflineValidation(false);
      try {
        const validatedValue = await api.validateShareKey(validateRequestId, clean);
        if (requestGeneration !== generation.current) return;
        validateRequest.current.reset();
        setPreview(validatedValue);
        setValidated(validatedValue.issuance === "validated");
      } catch (cause) {
        if (requestGeneration !== generation.current) return;
        if (
          cause instanceof Error &&
          "code" in cause &&
          (cause as { code?: string }).code === "offline"
        ) {
          setOfflineValidation(true);
          setError("소유자가 오프라인입니다. 폴더를 선택하면 가입 요청을 안전한 대기로 저장할 수 있습니다.");
        } else {
          setError(safeShareError(cause, clean));
        }
      }
    } catch (cause) {
      if (requestGeneration === generation.current)
        setError(safeShareError(cause, clean));
    } finally {
      if (requestGeneration === generation.current) setBusy(null);
    }
  }

  async function submitJoin() {
    if (joinSecretExpired) {
      setJoinError("공유 키가 만료되어 새 요청을 보낼 수 없습니다. 공유 보드에서 상태를 확인하세요.");
      return;
    }
    if (!preview || (!validated && !offlineValidation) || !destination.trim() || busy) {
      setJoinError(
        offlineValidation
          ? "이 장치의 저장 폴더를 선택하세요. 소유자 연결이 복구되면 처리됩니다."
          : "키를 확인하고 이 장치의 저장 폴더를 선택하세요.",
      );
      return;
    }
    const requestGeneration = ++generation.current;
    const cleanDestination = destination.trim();
    const requestId = joinRequest.current.begin(
      operationFingerprint(preview.share_id, key.trim(), cleanDestination),
    );
    setBusy("join");
    setJoinError("");
    try {
      const value = await api.joinShare(requestId, key.trim(), cleanDestination);
      if (requestGeneration !== generation.current) return;
      setResultName(preview.name);
      setResult(value);
      // Enrollment success/pending both clear the bearer from React state.
      setKey("");
      setJoinSecretExpired(false);
      setPreview(null);
      setValidated(false);
      setOfflineValidation(false);
      previewRequest.current.reset();
      validateRequest.current.reset();
      joinRequest.current.reset();
      await onComplete();
    } catch (cause) {
      if (requestGeneration === generation.current)
        setJoinError(safeShareError(cause, key.trim()));
    } finally {
      if (requestGeneration === generation.current) setBusy(null);
    }
  }

  function join(event: FormEvent) {
    event.preventDefault();
    void submitJoin();
  }

  useEffect(() => {
    if (preview?.expires_at == null) return;
    return scheduleExpiry(preview.expires_at, () => {
      if (busy === "join") {
        // Do not invalidate a join already accepted by the server. Clear the
        // bearer while allowing its authoritative response to render.
        setKey("");
        setJoinSecretExpired(true);
        setJoinError("공유 키가 만료되었습니다. 가입 결과를 확인하는 중입니다.");
        return;
      }
      generation.current += 1;
      setKey("");
      setJoinSecretExpired(false);
      setPreview(null);
      setValidated(false);
      setOfflineValidation(false);
      setDestination("");
      setBusy(null);
      setJoinError("");
      setError("공유 키가 만료되어 화면에서 지웠습니다.");
      previewRequest.current.reset();
      validateRequest.current.reset();
      joinRequest.current.reset();
    });
  }, [busy, preview?.expires_at]);

  return (
    <form onSubmit={join} className="share-form join-form">
      {!result ? (
        <>
          <div className="notice">
            <LinkSimple size={18} />
            <span>
              키는 이 브라우저 메모리에서만 처리합니다. 먼저 서명 정보를 확인한 뒤 소유자에게
              온라인 검증을 요청하고, 이 장치의 저장 폴더를 선택합니다.
            </span>
          </div>
          <Field label="공유 키" help="전달받은 공유 키를 붙여 넣으세요. 가입이 끝나거나 창을 닫으면 지웁니다.">
            <textarea
              className="mono share-key-input"
              value={key}
              onChange={(event) => changeKey(event.target.value)}
              placeholder="공유 키 붙여넣기"
              rows={4}
              autoFocus
              spellCheck={false}
              data-sensitive="share-key"
              disabled={busy === "join"}
              required
            />
          </Field>
          <div className="share-step-actions">
            <button
              type="button"
              className="btn subtle"
              onClick={checkKey}
              disabled={!key.trim() || busy !== null}
            >
              {busy === "check" ? <CircleNotch className="spinner" size={16} /> : <ShieldCheck size={16} />}
              {validated ? "키 확인 완료" : "키 확인"}
            </button>
          </div>
          {error && <ErrorBox error={error} />}
          {preview && (
            <div className="key-preview-card">
              <div className="key-preview-head">
                <div className="item-icon"><Key size={21} /></div>
                <div>
                  <span className="eyebrow">서명 확인 결과</span>
                  <strong>{preview.name}</strong>
                </div>
                <PermissionBadge permission={preview.permission} />
              </div>
              <dl className="details-grid compact-details">
                <div><dt>공유 ID</dt><dd className="mono key-id">{shortOpaqueId(preview.share_id)}</dd></div>
                <div><dt>초대 만료</dt><dd>{formatExpiry(preview.expires_at)}</dd></div>
              </dl>
              <p className="muted">
                {validated
                  ? "소유자 발급 기록이 확인되었습니다. 이제 이 장치의 저장 폴더를 선택하세요."
                  : offlineValidation
                    ? "소유자 연결이 복구되면 가입 요청을 다시 확인할 수 있습니다."
                    : "이 정보는 서명만 확인한 결과이며 아직 가입되거나 동기화되지 않았습니다."}
              </p>
            </div>
          )}
          {preview && (validated || offlineValidation) && (
            <ShareDirectoryPicker
              label="이 장치에 저장할 폴더"
              help="폴더 경로는 이 웹 콘솔을 실행하는 기기의 서버 폴더입니다. 선택 후 실제 가입 요청이 전송됩니다."
              value={destination}
              onChange={setDestination}
              browse={browse}
              disabled={busy !== null}
            />
          )}
          {joinError && <ErrorBox error={joinError} />}
          {joinError && !joinSecretExpired && (
            <button
              type="button"
              className="btn subtle small join-retry"
              onClick={() => void submitJoin()}
              disabled={busy !== null}
            >
              같은 요청 재시도
            </button>
          )}
          <div className="form-actions">
            <span className="muted">미리보기 → 소유자 확인 → 폴더 선택 → 가입</span>
            <button
              type="submit"
              className="btn primary"
              disabled={!preview || (!validated && !offlineValidation) || !destination.trim() || busy !== null}
            >
              {busy === "join" ? <CircleNotch className="spinner" size={16} /> : <LinkSimple size={16} />}
              {busy === "join" ? "가입 중…" : offlineValidation ? "가입 대기로 저장" : "공유에 가입"}
            </button>
          </div>
        </>
      ) : (
        <div className="join-result">
          <div className="share-success-banner">
            {result.enrollment === "enrolled" ? <CheckCircle size={22} weight="fill" /> : <Info size={22} />}
            <div>
              <strong>
                {result.enrollment === "enrolled"
                  ? "공유에 가입했습니다."
                  : result.enrollment === "revoked"
                    ? "이 공유에 가입할 수 없습니다."
                    : result.enrollment === "error"
                      ? "가입 결과를 확인해야 합니다."
                      : "가입 요청을 저장했습니다."}
              </strong>
              <p>
                {result.enrollment === "enrolled"
                  ? "소유자의 최신 상태를 확인한 뒤 초기 동기화를 시작합니다."
                  : result.enrollment === "revoked"
                    ? "공유 권한이 철회되어 이 장치의 가입을 진행할 수 없습니다."
                    : result.enrollment === "error"
                      ? "공유 보드에서 상태를 확인한 뒤 다시 시도하세요."
                      : "소유자 연결이 복구되면 공유 보드에서 가입 요청을 다시 확인할 수 있습니다."}
              </p>
            </div>
          </div>
          <div className="join-result-card">
            <span className="eyebrow">가입 결과</span>
            <strong>{resultName || "관리형 공유"}</strong>
            <div className="join-result-meta">
              <ShareStatus status={result.status} />
              <PermissionBadge permission={result.permission} />
            </div>
          </div>
          <div className="permission-note">
            <ShieldCheck size={15} />
            <span>응답이 늦거나 끊겨도 같은 가입 요청으로 확인할 수 있으며, 키는 더 이상 보관하지 않습니다.</span>
          </div>
          <div className="form-actions">
            <span className="muted">공유 보드에서 상태와 재개 가능 여부를 확인하세요.</span>
            <button type="button" className="btn primary" onClick={onClose}>닫기 <Check size={16} /></button>
          </div>
        </div>
      )}
    </form>
  );
}

function ShareCommandButtons({
  share,
  api,
  onRefresh,
}: {
  share: ManagedShareView;
  api: Api;
  onRefresh: () => Promise<unknown>;
}) {
  const [busy, setBusy] = useState<string | null>(null);
  const [error, setError] = useState("");
  const requests = useRef(new Map<string, OperationRequest>());
  const mounted = useRef(true);
  useEffect(
    () => () => {
      mounted.current = false;
      requests.current.forEach((request) => request.reset());
    },
    [],
  );
  async function command(value: "sync" | "pause" | "resume") {
    if (busy || !mounted.current) return;
    const requestGeneration = share.share_id;
    const tracker = requests.current.get(value) ?? new OperationRequest();
    requests.current.set(value, tracker);
    const requestId = tracker.begin(operationFingerprint(requestGeneration, value));
    setBusy(value);
    setError("");
    try {
      await api.shareCommand(requestGeneration, value, requestId);
      if (!mounted.current) return;
      tracker.reset();
      await onRefresh();
    } catch (cause) {
      if (mounted.current) setError(safeShareError(cause));
    } finally {
      if (mounted.current) setBusy(null);
    }
  }
  const paused = share.status === "paused";
  return (
    <div className="managed-actions">
      <button
        type="button"
        className="icon-btn"
        aria-label={`${share.name} 지금 동기화`}
        title="지금 동기화"
        disabled={busy !== null || share.status === "revoked" || share.status === "paused"}
        onClick={() => command("sync")}
      >
        {busy === "sync" ? <CircleNotch className="spinner" size={16} /> : <ArrowsClockwise size={16} />}
      </button>
      <button
        type="button"
        className="icon-btn"
        aria-label={`${share.name} ${paused ? "재개" : "일시정지"}`}
        title={paused ? "재개" : "일시정지"}
        disabled={busy !== null || share.status === "revoked"}
        onClick={() => command(paused ? "resume" : "pause")}
      >
        {busy === "pause" || busy === "resume" ? <CircleNotch className="spinner" size={16} /> : paused ? <Play size={16} /> : <Pause size={16} />}
      </button>
      {error && <span className="inline-error" role="alert">{error}</span>}
    </div>
  );
}

function ShareMetrics({ share }: { share: ManagedShareView }) {
  return (
    <div className="managed-metrics">
      <div><span>전송량</span><strong>{bytes(share.transferred_bytes)}</strong></div>
      <div><span>속도 (바이트/초)</span><strong>{bytes(share.speed_bps)} /s</strong></div>
      <div><span>연결 중</span><strong>{number(share.active_peer_count)}대</strong></div>
      <div><span>파일</span><strong>{number(share.files_count)}개</strong></div>
    </div>
  );
}

function ManagedErrorNotice({ error }: { error: ManagedShareView["last_error"] }) {
  if (!error) return null;
  return (
    <div className="managed-error-summary" role="alert">
      <Info size={15} />
      <span>{managedErrorMessages[error.code] ?? "공유 상태를 확인할 수 없습니다."}</span>
    </div>
  );
}

export function ManagedSharesBoard({
  shares,
  pending,
  api,
  onCreate,
  onJoin,
  onDetail,
  onRefresh,
}: {
  shares: ManagedShareView[];
  pending: PendingView[];
  api: Api;
  onCreate: () => void;
  onJoin: () => void;
  onDetail: (share: ManagedShareView) => void;
  onRefresh: () => Promise<unknown>;
}) {
  const empty = !shares.length && !pending.length;
  return (
    <section className="panel managed-board" aria-labelledby="managed-board-title">
      <div className="panel-head managed-board-head">
        <div>
          <div className="managed-title-line">
            <span className="managed-mark"><LinkSimple size={16} /></span>
            <span className="eyebrow">MANAGED SYNC</span>
          </div>
          <h2 id="managed-board-title">관리형 공유</h2>
          <p>키로 연결한 공유의 실제 동기화 상태와 연결 기기를 확인합니다.</p>
        </div>
        <div className="managed-board-actions">
          <button type="button" className="btn subtle small" onClick={onJoin}><LinkSimple size={15} />키로 연결</button>
          <button type="button" className="btn primary small" onClick={onCreate}><Plus size={15} />폴더 공유</button>
        </div>
      </div>
      {empty ? (
        <Empty
          compact
          icon={<LinkSimple size={24} />}
          title="아직 관리형 공유가 없습니다"
          description="폴더를 공유해 키를 발급하거나, 전달받은 키로 이 장치를 연결하세요."
          action={<div className="empty-actions"><button type="button" className="btn primary small" onClick={onCreate}>폴더 공유</button><button type="button" className="btn subtle small" onClick={onJoin}>키로 연결</button></div>}
        />
      ) : (
        <div className="managed-list">
          {shares.map((share) => (
            <article className="managed-share-row" key={share.share_id}>
              <button type="button" className="managed-share-main" onClick={() => onDetail(share)}>
                <span className="item-icon managed-icon"><LinkSimple size={21} /></span>
                <span className="managed-share-copy">
                  <strong>{share.name}</strong>
                  <code>{share.root}</code>
                  <span className="managed-share-subline"><span>{roleLabel(share.role)}</span><span>·</span><span>{permissionLabel(share.permission)}</span></span>
                </span>
                <ShareStatus status={share.status} />
              </button>
              <ShareMetrics share={share} />
              <ManagedErrorNotice error={share.last_error} />
              <div className="managed-peer-summary">
                <UsersThree size={16} />
                <span>{share.active_peer_count > 0 ? `현재 ${share.active_peer_count}대 연결 중` : "현재 연결 없음"}</span>
                <small>등록 멤버 수는 상세에서 확인 · 현재 연결과 별도 집계</small>
              </div>
              <ShareCommandButtons share={share} api={api} onRefresh={onRefresh} />
              <button type="button" className="icon-btn managed-detail-button" aria-label={`${share.name} 공유 상세`} onClick={() => onDetail(share)}><ArrowRight size={17} /></button>
            </article>
          ))}
          {pending.map((item) => (
            <PendingShareRow key={`${item.share_id}:${item.request_id}`} item={item} api={api} onRefresh={onRefresh} />
          ))}
        </div>
      )}
      {!empty && <div className="panel-foot managed-foot"><Info size={14} /> 등록된 멤버 수는 온라인 기기 수로 표시하지 않습니다. 현재 처리 중인 연결만 연결 수에 반영합니다.</div>}
    </section>
  );
}

function PendingShareRow({
  item,
  api,
  onRefresh,
}: {
  item: PendingView;
  api: Api;
  onRefresh: () => Promise<unknown>;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const request = useRef(new OperationRequest());
  const mounted = useRef(true);
  useEffect(
    () => () => {
      mounted.current = false;
      request.current.reset();
    },
    [],
  );
  async function resume() {
    if (busy || !mounted.current) return;
    const id = request.current.begin(operationFingerprint(item.share_id));
    setBusy(true);
    setError("");
    try {
      await api.resumeMembership(id, item.share_id);
      if (!mounted.current) return;
      request.current.reset();
      await onRefresh();
    } catch (cause) {
      if (mounted.current) setError(safeShareError(cause));
    } finally {
      if (mounted.current) setBusy(false);
    }
  }
  return (
    <article className="managed-share-row pending-share-row">
      <span className="item-icon pending-icon"><ClockIcon /></span>
      <span className="managed-share-copy"><strong>가입 대기 중인 공유</strong><code title={item.share_id}>{shortOpaqueId(item.share_id)}</code><span className="managed-share-subline">소유자 응답을 기다리는 가입 대기</span></span>
      <ShareStatus status={item.status} />
      <span className="pending-retry">{item.retry_at != null ? `다음 확인 ${date(item.retry_at * 1000)}` : "재개 가능"}</span>
      <button type="button" className="btn subtle small" onClick={resume} disabled={busy}>{busy ? <CircleNotch className="spinner" size={15} /> : <ArrowsClockwise size={15} />}{busy ? "확인 중…" : "가입 재개"}</button>
      {error && <span className="inline-error" role="alert">{error}</span>}
    </article>
  );
}

function ClockIcon() {
  return <Clock size={21} aria-hidden="true" />;
}

export function ShareDetailFlow({
  share,
  api,
  onClose,
  onRefresh,
}: {
  share: ManagedShareView;
  api: Api;
  onClose: () => void;
  onRefresh: () => Promise<unknown>;
}) {
  const [keys, setKeys] = useState<KeySummary[]>([]);
  const [members, setMembers] = useState<MemberView[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [issued, setIssued] = useState<IssuedKey | null>(null);
  const [action, setAction] = useState("");
  const [actionError, setActionError] = useState("");
  const [notice, setNotice] = useState("");
  const mounted = useRef(true);
  const generation = useRef(0);
  const mutationRequests = useRef(new Map<string, OperationRequest>());

  const reload = useCallback(async () => {
    const requestGeneration = generation.current;
    if (!mounted.current) return;
    setLoading(true);
    setError("");
    try {
      const nextKeys = share.role === "owner" ? await api.listKeys(share.share_id) : [];
      const nextMembers = share.role === "owner" ? await api.listMembers(share.share_id) : [];
      if (!mounted.current || requestGeneration !== generation.current) return;
      setKeys(nextKeys);
      setMembers(nextMembers);
    } catch (cause) {
      if (mounted.current && requestGeneration === generation.current) setError(safeShareError(cause));
    } finally {
      if (mounted.current && requestGeneration === generation.current) setLoading(false);
    }
  }, [api, share.role, share.share_id]);

  useEffect(() => {
    const requestGeneration = ++generation.current;
    mounted.current = true;
    setIssued(null);
    setAction("");
    setActionError("");
    setNotice("");
    void reload();
    return () => {
      mounted.current = false;
      if (requestGeneration === generation.current) generation.current += 1;
      mutationRequests.current.forEach((request) => request.reset());
    };
  }, [reload]);

  useEffect(() => {
    if (issued?.expires_at == null) return;
    const requestGeneration = generation.current;
    return scheduleExpiry(issued.expires_at, () => {
      if (!mounted.current || requestGeneration !== generation.current) return;
      generation.current += 1;
      setIssued(null);
      setActionError("발급된 키가 만료되어 화면에서 지웠습니다.");
      mutationRequests.current.forEach((request) => request.reset());
    });
  }, [issued]);

  async function issue(permission: Permission) {
    if (action || !mounted.current) return;
    const requestGeneration = generation.current;
    const operation = `issue:${permission}`;
    const tracker = mutationRequests.current.get(operation) ?? new OperationRequest();
    mutationRequests.current.set(operation, tracker);
    const id = tracker.begin(operationFingerprint(share.share_id, permission));
    setAction(operation);
    setActionError("");
    try {
      const value = await api.issueKey(share.share_id, id, permission);
      if (!mounted.current || requestGeneration !== generation.current) return;
      tracker.reset();
      setIssued(value);
      await onRefresh();
    } catch (cause) {
      if (mounted.current && requestGeneration === generation.current) setActionError(safeShareError(cause));
    } finally {
      if (mounted.current && requestGeneration === generation.current) setAction("");
    }
  }

  async function rotate(invitationId: string) {
    if (action || !mounted.current) return;
    const requestGeneration = generation.current;
    const operation = `rotate:${invitationId}`;
    const tracker = mutationRequests.current.get(operation) ?? new OperationRequest();
    mutationRequests.current.set(operation, tracker);
    const id = tracker.begin(operationFingerprint(share.share_id, invitationId));
    setAction(operation);
    setActionError("");
    try {
      const value = await api.rotateKey(share.share_id, invitationId, id);
      if (!mounted.current || requestGeneration !== generation.current) return;
      tracker.reset();
      setIssued(value);
      await reload();
      if (!mounted.current || requestGeneration !== generation.current) return;
      await onRefresh();
    } catch (cause) {
      if (mounted.current && requestGeneration === generation.current) setActionError(safeShareError(cause));
    } finally {
      if (mounted.current && requestGeneration === generation.current) setAction("");
    }
  }

  async function revokeKey(invitationId: string) {
    if (action || !mounted.current) return;
    const requestGeneration = generation.current;
    const operation = `revoke-key:${invitationId}`;
    const tracker = mutationRequests.current.get(operation) ?? new OperationRequest();
    mutationRequests.current.set(operation, tracker);
    const id = tracker.begin(operationFingerprint(share.share_id, invitationId));
    setAction(operation);
    setActionError("");
    try {
      const value = await api.revokeKey(share.share_id, invitationId, id);
      if (!mounted.current || requestGeneration !== generation.current) return;
      if (value.completion === "complete") tracker.reset();
      if (value.completion === "complete") {
        setNotice("키를 철회했습니다. 이 키로 새 가입을 시작할 수 없습니다. 이미 전달된 파일은 회수되지 않습니다.");
      } else {
        setNotice("키 철회 상태를 확인하는 중입니다. 완료 전에는 철회 완료로 표시하지 않습니다.");
      }
      await reload();
      if (!mounted.current || requestGeneration !== generation.current) return;
      await onRefresh();
    } catch (cause) {
      if (mounted.current && requestGeneration === generation.current) setActionError(safeShareError(cause));
    } finally {
      if (mounted.current && requestGeneration === generation.current) setAction("");
    }
  }

  async function revokeMember(memberId: string) {
    if (action || !mounted.current) return;
    const requestGeneration = generation.current;
    const operation = `revoke-member:${memberId}`;
    const tracker = mutationRequests.current.get(operation) ?? new OperationRequest();
    mutationRequests.current.set(operation, tracker);
    const id = tracker.begin(operationFingerprint(share.share_id, memberId));
    setAction(operation);
    setActionError("");
    try {
      const value = await api.revokeMember(share.share_id, memberId, id);
      if (!mounted.current || requestGeneration !== generation.current) return;
      if (value.completion === "complete") tracker.reset();
      if (value.completion === "pending") {
        setNotice("신규 권한은 막혔습니다. 연결 종료 확인 중이며, 완료 전까지 철회 완료로 표시하지 않습니다.");
      } else if (value.completion === "complete") {
        setNotice("멤버 철회가 완료되었습니다. 이후 전송과 쓰기는 차단됩니다.");
      } else {
        setNotice("멤버 철회 상태를 확인할 수 없습니다. 완료 전에는 철회 완료로 표시하지 않습니다.");
      }
      await reload();
      if (!mounted.current || requestGeneration !== generation.current) return;
      await onRefresh();
    } catch (cause) {
      if (mounted.current && requestGeneration === generation.current) setActionError(safeShareError(cause));
    } finally {
      if (mounted.current && requestGeneration === generation.current) setAction("");
    }
  }

  return (
    <div className="share-detail-flow">
      <div className="share-detail-hero">
        <div className="item-icon managed-icon"><LinkSimple size={24} /></div>
        <div className="share-detail-title"><span className="eyebrow">{roleLabel(share.role)} · {permissionLabel(share.permission)}</span><h3>{share.name}</h3><code>{share.root}</code></div>
      <ShareStatus status={share.status} />
      </div>
      <ShareMetrics share={share} />
      <ManagedErrorNotice error={share.last_error} />
      <div className="share-detail-connections">
        <div className="detail-section-title"><UsersThree size={16} /><strong>현재 연결</strong><span>{share.active_peer_count}대</span></div>
        {share.connected_devices.length ? share.connected_devices.map((device) => (
          <div className="connected-device-row" key={device.member_id}>
            <UserCircle size={20} /><span className="connected-device-name">기기 멤버</span><PermissionBadge permission={device.permission} /><span>{device.active_operations}개 작업 · {device.last_seen_at != null ? ago(device.last_seen_at * 1000) : "최근 기록 없음"}</span>
          </div>
        )) : <p className="muted detail-empty">현재 인증된 작업이 없습니다. 등록 멤버 수는 현재 연결로 간주하지 않습니다.</p>}
      </div>
      {error && <ErrorBox error={error} />}
      {notice && <div className="notice" role="status"><Info size={16} /><span>{notice}</span></div>}
      {actionError && <ErrorBox error={actionError} />}
      {share.role === "owner" && (
        <>
          <section className="share-management-section" aria-labelledby="share-keys-heading">
            <div className="detail-section-title"><Key size={16} /><strong id="share-keys-heading">발급 키</strong><span>{keys.length}개</span></div>
            <div className="key-choice-inline">
              {(["read_only", "read_write"] as Permission[]).map((permission) => (
                <button type="button" className="btn subtle small" key={permission} disabled={!!action} onClick={() => issue(permission)}>{action === `issue:${permission}` ? <CircleNotch className="spinner" size={14} /> : <Plus size={14} />}{permissionLabel(permission)} 키 발급</button>
              ))}
            </div>
            {loading ? <p className="muted">키 목록을 불러오는 중…</p> : keys.length ? <div className="key-summary-list">{keys.map((item) => <KeySummaryRow key={item.invitation_id} item={item} action={action} onRotate={rotate} onRevoke={revokeKey} />)}</div> : <p className="muted detail-empty">아직 발급한 키가 없습니다.</p>}
          </section>
          <section className="share-management-section" aria-labelledby="share-members-heading">
            <div className="detail-section-title"><UsersThree size={16} /><strong id="share-members-heading">멤버</strong><span>{members.length}명</span></div>
            {loading ? <p className="muted">멤버 목록을 불러오는 중…</p> : members.length ? <div className="member-list">{members.map((member) => <MemberRow key={member.member_id} item={member} action={action} onRevoke={revokeMember} />)}</div> : <p className="muted detail-empty">아직 가입한 멤버가 없습니다.</p>}
          </section>
        </>
      )}
      {issued && (
        <div className="issued-key-panel" data-sensitive="share-key">
          <div className="issued-key-head"><div><span className="eyebrow">한 번만 표시</span><strong>새 {permissionLabel(issued.permission)} 키</strong></div><PermissionBadge permission={issued.permission} /></div>
          <code className="share-key-value">{issued.key}</code>
          <div className="copy-row"><CopyButton value={issued.key} label="키 복사" /><button type="button" className="btn subtle small" onClick={() => setIssued(null)}>키 숨기기</button></div>
        </div>
      )}
      <div className="permission-note"><ShieldCheck size={15} /><span>키 철회는 신규 가입을 막고, 멤버 철회는 신규 권한을 막은 뒤 기존 연결·쓰기 종료를 확인합니다. 분할 중에는 완료를 추측하지 않습니다.</span></div>
      <div className="form-actions"><span className="muted">발급한 키는 이 창에서만 확인할 수 있습니다.</span><button type="button" className="btn primary" onClick={onClose}>닫기 <Check size={16} /></button></div>
    </div>
  );
}

function KeySummaryRow({
  item,
  action,
  onRotate,
  onRevoke,
}: {
  item: KeySummary;
  action: string;
  onRotate: (id: string) => Promise<void>;
  onRevoke: (id: string) => Promise<void>;
}) {
  return (
    <div className={`key-summary-row ${item.revoked_at != null ? "revoked" : ""}`}>
      <div className="key-summary-icon"><Key size={16} /></div>
      <div className="key-summary-copy"><strong>{permissionLabel(item.permission)}</strong><span className="mono" title={item.invitation_id}>초대 {shortOpaqueId(item.invitation_id)}</span><small>{item.revoked_at != null ? `철회 ${date(item.revoked_at * 1000)}` : `${formatExpiry(item.expires_at)} · 발급 ${item.issued_at != null ? date(item.issued_at * 1000) : "시각 미기록"}`}</small></div>
      <div className="key-summary-actions">{!item.revoked_at && <><button type="button" className="btn subtle small" disabled={!!action} onClick={() => onRotate(item.invitation_id)}>{action === `rotate:${item.invitation_id}` ? <CircleNotch className="spinner" size={14} /> : <ArrowsClockwise size={14} />}교체</button><button type="button" className="btn danger small" disabled={!!action} onClick={() => onRevoke(item.invitation_id)}>{action === `revoke-key:${item.invitation_id}` ? <CircleNotch className="spinner" size={14} /> : <Trash size={14} />}철회</button></>}</div>
    </div>
  );
}

function MemberRow({
  item,
  action,
  onRevoke,
}: {
  item: MemberView;
  action: string;
  onRevoke: (id: string) => Promise<void>;
}) {
  const pending = item.revocation_pending;
  const revoked = item.revoked_at != null;
  return (
    <div className={`member-row ${revoked ? "revoked" : ""}`}>
      <div className="member-avatar"><UserCircle size={20} /></div>
      <div className="member-copy"><strong>기기 멤버</strong><span className="mono" title={item.member_id}>{shortOpaqueId(item.member_id)}</span><small>가입 {date(item.enrolled_at * 1000)} · 현재 처리 중인 작업 {item.active_operations}개</small></div>
      <PermissionBadge permission={item.permission} />
      <div className="member-actions">{pending ? <span className="pending-revocation"><CircleNotch className="spinner" size={14} />연결 종료 확인 중</span> : revoked ? <span className="revoked-label">철회 완료</span> : <button type="button" className="btn danger small" disabled={!!action} onClick={() => onRevoke(item.member_id)}>{action === `revoke-member:${item.member_id}` ? <CircleNotch className="spinner" size={14} /> : <Trash size={14} />}멤버 철회</button>}</div>
    </div>
  );
}

export function ShareRemoveButton({
  share,
  api,
  onRemoved,
}: {
  share: ManagedShareView;
  api: Api;
  onRemoved: () => Promise<unknown>;
}) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const request = useRef(new OperationRequest());
  const mounted = useRef(true);
  useEffect(
    () => () => {
      mounted.current = false;
      request.current.reset();
    },
    [],
  );
  async function remove() {
    if (busy || !mounted.current) return;
    const id = request.current.begin(operationFingerprint(share.share_id, "remove"));
    setBusy(true);
    setError("");
    try {
      const result = await api.removeShare(share.share_id, id);
      if (!mounted.current) return;
      if (result.completion !== "complete") {
        setError("공유 작업 종료를 기다리는 중입니다. 완료 전에는 제거로 표시하지 않습니다.");
        return;
      }
      request.current.reset();
      await onRemoved();
    } catch (cause) {
      if (mounted.current) setError(safeShareError(cause));
    } finally {
      if (mounted.current) setBusy(false);
    }
  }
  return <div className="danger-zone share-danger-zone"><div><strong>관리형 공유 연결 해제</strong><p>공유 연결을 해제합니다. 저장된 파일과 복구 데이터는 유지됩니다.</p></div><button type="button" className="btn danger small" disabled={busy} onClick={remove}>{busy ? <CircleNotch className="spinner" size={14} /> : <Trash size={14} />}공유 연결 해제</button>{error && <ErrorBox error={error} />}</div>;
}

export function ShareOverviewCard({ share }: { share: ManagedShareView }) {
  return <div className="share-overview-card"><div className="share-overview-card-head"><span className="eyebrow">{roleLabel(share.role)}</span><ShareStatus status={share.status} /></div><strong>{share.name}</strong><span>{bytes(share.transferred_bytes)} 전송 · {share.active_peer_count}대 연결</span></div>;
}
