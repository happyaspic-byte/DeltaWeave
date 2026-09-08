import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
} from "react";
import {
  Pulse as ActivityIcon,
  ArrowDownLeft,
  ArrowRight,
  ArrowUpRight,
  ArrowsClockwise,
  CaretRight,
  ChartLineUp,
  Check,
  CheckCircle,
  CircleNotch,
  Clock,
  Desktop,
  DownloadSimple,
  File,
  Folder as FolderIcon,
  FolderPlus,
  GearSix,
  HardDrives,
  Info,
  List,
  LinkSimple,
  MagnifyingGlass,
  Pause,
  PencilSimple,
  Plus,
  Pulse,
  ShieldCheck,
  SignOut,
  SlidersHorizontal,
  Stack,
  Trash,
  Warning,
  WifiSlash,
} from "@phosphor-icons/react";
import { Api, consumeBootstrap } from "./api";
import {
  Brand,
  ConfirmRemove,
  CopyButton,
  DeviceForm,
  Empty,
  ErrorBox,
  Field,
  FolderControls,
  FolderForm,
  Login,
  Modal,
  Status,
  publicConnection,
} from "./components";
import { ago, bytes, date, errorMessage, number, time, uptime } from "./format";
import {
  ManagedSharesBoard,
  ShareCreateFlow,
  ShareDetailFlow,
  ShareJoinFlow,
  ShareRemoveButton,
} from "./share";
import type {
  Activity,
  AppSnapshot,
  DeviceView,
  Directory,
  FolderView,
  HistoryPoint,
  ManagedShareView,
  Settings,
} from "./types";

type Page = "overview" | "folders" | "devices" | "activity" | "settings";
type Dialog =
  | { kind: "folder-form"; folder?: FolderView }
  | { kind: "device-form"; device?: DeviceView }
  | { kind: "folder-detail"; id: string }
  | { kind: "activity-detail"; activity: Activity }
  | { kind: "remove-folder"; folder: FolderView }
  | { kind: "remove-device"; device: DeviceView }
  | { kind: "share-create" }
  | { kind: "share-join" }
  | { kind: "share-detail"; share: ManagedShareView }
  | null;
const pages: {
  id: Page;
  label: string;
  icon: typeof ChartLineUp;
  description: string;
}[] = [
  {
    id: "overview",
    label: "개요",
    icon: ChartLineUp,
    description: "장치와 폴더의 흐름을 한눈에 확인하세요.",
  },
  {
    id: "folders",
    label: "폴더",
    icon: FolderIcon,
    description: "연결된 폴더와 동기화 작업을 관리하세요.",
  },
  {
    id: "devices",
    label: "장치",
    icon: Desktop,
    description: "함께 연결할 장치의 공개 정보를 관리하세요.",
  },
  {
    id: "activity",
    label: "활동",
    icon: ActivityIcon,
    description: "실제 파일 작업과 동기화 결과를 확인하세요.",
  },
  {
    id: "settings",
    label: "설정",
    icon: GearSix,
    description: "이 장치의 동기화 환경을 설정하세요.",
  },
];

export default function App() {
  const [api] = useState(() => new Api());
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [loginError, setLoginError] = useState("");
  const [state, setState] = useState<AppSnapshot | null>(null);
  const [error, setError] = useState("");
  const [transport, setTransport] = useState<"connecting" | "live" | "offline">(
    "connecting",
  );
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const [page, setPage] = useState<Page>("overview");
  const [mobile, setMobile] = useState(false);
  const mobileToggleRef = useRef<HTMLButtonElement>(null);
  const navigationRef = useRef<HTMLElement>(null);
  useEffect(() => {
    if (!mobile) return;
    const nav = navigationRef.current;
    (
      nav?.querySelector<HTMLButtonElement>('[aria-current="page"]') ??
      nav?.querySelector<HTMLButtonElement>("button")
    )?.focus();
    function dismissOutside(event: PointerEvent) {
      if (
        event.target instanceof Node &&
        !nav?.contains(event.target) &&
        !mobileToggleRef.current?.contains(event.target)
      ) {
        setMobile(false);
      }
    }
    function dismissOnEscape(event: KeyboardEvent) {
      if (
        event.key !== "Escape" ||
        event.defaultPrevented ||
        document.querySelector('[role="dialog"]')
      )
        return;
      event.preventDefault();
      setMobile(false);
      mobileToggleRef.current?.focus();
    }
    document.addEventListener("pointerdown", dismissOutside);
    document.addEventListener("keydown", dismissOnEscape);
    return () => {
      document.removeEventListener("pointerdown", dismissOutside);
      document.removeEventListener("keydown", dismissOnEscape);
    };
  }, [mobile]);
  const [dialog, setDialog] = useState<Dialog>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [toast, setToast] = useState<{
    message: string;
    error?: boolean;
  } | null>(null);
  const [eventGeneration, setEventGeneration] = useState(0);
  const stateRef = useRef<AppSnapshot | null>(null);
  const authRef = useRef<boolean | null>(null);
  authRef.current = authenticated;
  const applySnapshot = useCallback((snapshot: AppSnapshot) => {
    if (
      stateRef.current &&
      snapshot.node.started_at === stateRef.current.node.started_at &&
      snapshot.revision < stateRef.current.revision
    )
      return;
    stateRef.current = snapshot;
    setState(snapshot);
    setLastUpdated(Date.now());
    setError("");
  }, []);
  const expire = useCallback(() => {
    authRef.current = false;
    setAuthenticated(false);
    stateRef.current = null;
    setState(null);
    setDialog(null);
    setLoginError("관리 세션이 만료되었습니다. 다시 로그인하세요.");
  }, []);
  const reload = useCallback(async () => {
    try {
      const snapshot = await api.request<AppSnapshot>("/state");
      if (authRef.current !== false) applySnapshot(snapshot);
    } catch (e) {
      setError(errorMessage(e));
      throw e;
    }
  }, [api, applySnapshot]);
  useEffect(() => {
    api.onUnauthorized = expire;
    let cancelled = false;
    (async () => {
      try {
        const session = (await consumeBootstrap(api)) ?? (await api.session());
        if (cancelled) return;
        authRef.current = session.authenticated;
        setAuthenticated(session.authenticated);
      } catch (e) {
        if (!cancelled) {
          setLoginError(errorMessage(e));
          setAuthenticated(false);
        }
      }
    })();
    return () => {
      cancelled = true;
      api.onUnauthorized = undefined;
    };
  }, [api, expire]);
  useEffect(() => {
    if (!authenticated) return;
    let disposed = false;
    let source: EventSource | null = null;
    let polling = false;
    setTransport("connecting");
    reload().catch(() => {});
    if (typeof EventSource !== "undefined") {
      source = new EventSource("/api/v1/events", { withCredentials: true });
      source.onopen = () => {
        if (!disposed) {
          setTransport("live");
          reload().catch(() => {});
        }
      };
      source.addEventListener("state", (event) => {
        try {
          const snapshot = JSON.parse(
            (event as MessageEvent).data,
          ) as AppSnapshot;
          if (!disposed) {
            applySnapshot(snapshot);
            setTransport("live");
          }
        } catch {
          if (!disposed) {
            setTransport("offline");
            setError("상태 정보를 읽지 못했습니다. 다시 연결하고 있습니다.");
          }
        }
      });
      source.onerror = () => {
        if (!disposed) setTransport("offline");
      };
    } else setTransport("offline");
    const interval = window.setInterval(async () => {
      if (polling || disposed) return;
      polling = true;
      try {
        await reload();
      } catch {
        if (!disposed) setTransport("offline");
      } finally {
        polling = false;
      }
    }, 15000);
    function offline() {
      setTransport("offline");
    }
    window.addEventListener("offline", offline);
    return () => {
      disposed = true;
      source?.close();
      clearInterval(interval);
      window.removeEventListener("offline", offline);
    };
  }, [authenticated, api, applySnapshot, reload, eventGeneration]);
  useEffect(() => {
    if (!toast) return;
    const timer = setTimeout(() => setToast(null), 4500);
    return () => clearTimeout(timer);
  }, [toast]);
  useEffect(() => {
    document.title = `DeltaWeave · ${pages.find((p) => p.id === page)?.label ?? "동기화 콘솔"}`;
  }, [page]);
  async function login(token: string) {
    await api.login(token);
    setLoginError("");
    authRef.current = true;
    setAuthenticated(true);
  }
  async function logout() {
    try {
      await api.logout();
      authRef.current = false;
      setAuthenticated(false);
      setState(null);
      stateRef.current = null;
      setDialog(null);
      setLoginError("");
    } catch (e) {
      setToast({ message: errorMessage(e), error: true });
    }
  }
  async function refresh() {
    setRefreshing(true);
    try {
      await reload();
      if (transport !== "live") setEventGeneration((n) => n + 1);
    } catch {
    } finally {
      setRefreshing(false);
    }
  }
  async function mutate(path: string, method: string, input?: unknown) {
    await api.request(path, {
      method,
      ...(input === undefined ? {} : { body: JSON.stringify(input) }),
    });
    try {
      await reload();
    } catch {
      throw new Error(
        "변경은 저장되었지만 최신 상태를 불러오지 못했습니다. 다시 연결해 확인하세요.",
      );
    }
  }
  const browse = useCallback(
    (path: string) =>
      api.request<Directory>(
        path.trim()
          ? `/browse?path=${encodeURIComponent(path.trim())}`
          : "/browse",
      ),
    [api],
  );
  async function command(
    folder: FolderView,
    command: "sync" | "pause" | "resume",
  ) {
    await api.command(folder.id, command);
    await reload();
  }
  function saved(message: string) {
    setDialog(null);
    setToast({ message });
  }
  function navigate(next: Page) {
    if (mobile) document.getElementById("main-content")?.focus();
    setPage(next);
    setMobile(false);
    window.scrollTo({ top: 0 });
  }
  if (authenticated === null)
    return (
      <div className="loading-page">
        <Brand />
        <CircleNotch className="spinner" size={24} />
        <p>관리 세션을 확인하고 있습니다.</p>
      </div>
    );
  if (!authenticated)
    return <Login onLogin={login} initialError={loginError} />;
  const current = pages.find((p) => p.id === page)!;
  const folderDetail =
    dialog?.kind === "folder-detail"
      ? state?.folders.find((f) => f.id === dialog.id)
      : undefined;
  const managedShares = state?.shares ?? [];
  const managedPending = state?.pending ?? [];
  const managedDetailShare =
    dialog?.kind === "share-detail"
      ? managedShares.find((share) => share.share_id === dialog.share.share_id) ??
        dialog.share
      : undefined;
  return (
    <div className="app">
      <header className="masthead">
        <div className="masthead-brand">
          <Brand />
        </div>
        <nav
          id="workspace-nav"
          ref={navigationRef}
          className={`nav masthead-nav ${mobile ? "open" : ""}`}
          aria-label="주 메뉴"
        >
          {pages.map((item) => (
            <button
              key={item.id}
              className={page === item.id ? "active" : ""}
              aria-current={page === item.id ? "page" : undefined}
              onClick={() => navigate(item.id)}
            >
              <item.icon
                size={18}
                weight={page === item.id ? "fill" : "regular"}
              />
              {item.label}
              {state && (item.id === "folders" || item.id === "devices") && (
                <span className="nav-count">{state[item.id].length}</span>
              )}
            </button>
          ))}
        </nav>
        <div className="masthead-controls">
          <div className="masthead-node">
            <HardDrives size={19} weight="duotone" />
            <div>
              <strong>{state?.node.name ?? "장치 확인 중"}</strong>
              <span>
                {state
                  ? `${state.node.platform} · 현재 장치`
                  : "워크스페이스 연결 중"}
              </span>
            </div>
          </div>
          <button
            className="icon-btn"
            aria-label="로그아웃"
            title="로그아웃"
            onClick={logout}
          >
            <SignOut size={19} />
          </button>
          <button
            className="icon-btn mobile-toggle"
            ref={mobileToggleRef}
            aria-label={mobile ? "메뉴 닫기" : "메뉴 열기"}
            aria-expanded={mobile}
            aria-controls="workspace-nav"
            onClick={() => setMobile(!mobile)}
          >
            <List size={23} />
          </button>
        </div>
      </header>
      <div className="main-shell">
        <main className="content" id="main-content" tabIndex={-1}>
          <div className="workspace-meta">
            <span className="workspace-label">
              나의 워크스페이스{" "}
              <span className="workspace-version">
                / v{state?.node.version ?? "…"}
              </span>
            </span>
            <span
              className={`transport ${transport === "live" ? "" : "offline"}`}
            >
              <span className="dot" />
              {transport === "live"
                ? "실시간 상태 연결"
                : transport === "connecting"
                  ? "상태 연결 중"
                  : "실시간 연결 끊김"}
            </span>
          </div>
          <div className="page-heading">
            <div>
              <h1>
                {page === "overview" ? "모든 연결, 한곳에서." : current.label}
              </h1>
              <p>{current.description}</p>
            </div>
            <div className="heading-actions">
              <button
                className="btn subtle hide-mobile"
                onClick={refresh}
                disabled={refreshing}
                aria-label="상태 새로고침"
              >
                <ArrowsClockwise
                  size={15}
                  className={refreshing ? "spinner" : ""}
                />
                <span>새로고침</span>
              </button>
              {page === "overview" || page === "folders" ? (
                <>
                  <button
                    type="button"
                    className="btn primary"
                    onClick={() => setDialog({ kind: "share-create" })}
                    disabled={!state}
                  >
                    <Plus size={17} />
                    폴더 공유
                  </button>
                  <button
                    type="button"
                    className="btn subtle"
                    onClick={() => setDialog({ kind: "share-join" })}
                    disabled={!state}
                  >
                    <LinkSimple size={17} />
                    키로 연결
                  </button>
                  <div className="advanced-action">
                    <span>고급 연결</span>
                    <button
                      type="button"
                      className="text-button"
                      aria-label="폴더 추가"
                      title="기존 수동 폴더 연결"
                      onClick={() => setDialog({ kind: "folder-form" })}
                      disabled={!state}
                    >
                      폴더 추가
                    </button>
                  </div>
                </>
              ) : page === "devices" ? (
                <button
                  className="btn primary"
                  onClick={() => setDialog({ kind: "device-form" })}
                  disabled={!state}
                >
                  <Plus size={17} />
                  장치 추가
                </button>
              ) : null}
            </div>
          </div>
          {(transport === "offline" || error) && (
            <div
              className={`notice ${error ? "error" : "warning"}`}
              role="status"
            >
              <WifiSlash size={19} />
              <span>
                {error ||
                  "실시간 연결이 끊겼습니다. 마지막 상태를 표시하며 자동으로 다시 연결합니다."}
                {lastUpdated && (
                  <small className="notice-timestamp">
                    마지막 상태 확인 {date(lastUpdated)}
                  </small>
                )}
              </span>
              <button
                className="text-button"
                onClick={refresh}
                disabled={refreshing}
              >
                다시 연결
              </button>
            </div>
          )}
          {!state ? (
            <div className="panel">
              <Empty
                title={
                  error
                    ? "상태를 불러오지 못했습니다"
                    : "장치 상태를 불러오는 중"
                }
                description={
                  error
                    ? "연결을 확인한 뒤 다시 시도하세요."
                    : "현재 장치의 폴더와 활동을 확인하고 있습니다."
                }
                icon={
                  error ? (
                    <WifiSlash size={25} />
                  ) : (
                    <CircleNotch className="spinner" size={25} />
                  )
                }
                action={
                  error ? (
                    <button
                      className="btn primary"
                      onClick={refresh}
                      disabled={refreshing}
                    >
                      다시 시도
                    </button>
                  ) : null
                }
              />
            </div>
          ) : (
            <>
              {page === "overview" && (
                <>
                  <Overview
                    state={state}
                    transport={transport}
                    onAdd={() => setDialog({ kind: "share-create" })}
                    onNavigate={navigate}
                    onDetail={(id) => setDialog({ kind: "folder-detail", id })}
                    onActivity={(activity) =>
                      setDialog({ kind: "activity-detail", activity })
                    }
                    onCommand={command}
                  />
                  <ManagedSharesBoard
                    shares={managedShares}
                    pending={managedPending}
                    api={api}
                    onCreate={() => setDialog({ kind: "share-create" })}
                    onJoin={() => setDialog({ kind: "share-join" })}
                    onDetail={(share) => setDialog({ kind: "share-detail", share })}
                    onRefresh={reload}
                  />
                </>
              )}
              {page === "folders" && (
                <>
                  <ManagedSharesBoard
                    shares={managedShares}
                    pending={managedPending}
                    api={api}
                    onCreate={() => setDialog({ kind: "share-create" })}
                    onJoin={() => setDialog({ kind: "share-join" })}
                    onDetail={(share) => setDialog({ kind: "share-detail", share })}
                    onRefresh={reload}
                  />
                  <FoldersPage
                    state={state}
                    onAdd={() => setDialog({ kind: "share-create" })}
                    onDetail={(id) => setDialog({ kind: "folder-detail", id })}
                    onCommand={command}
                  />
                </>
              )}
              {page === "devices" && (
                <DevicesPage
                  state={state}
                  onAdd={() => setDialog({ kind: "device-form" })}
                  onEdit={(device) =>
                    setDialog({ kind: "device-form", device })
                  }
                  onRemove={(device) =>
                    setDialog({ kind: "remove-device", device })
                  }
                />
              )}
              {page === "activity" && (
                <ActivityPage
                  state={state}
                  api={api}
                  onDetail={(activity) =>
                    setDialog({ kind: "activity-detail", activity })
                  }
                />
              )}
              {page === "settings" && (
                <SettingsPage
                  state={state}
                  onSave={(settings) => mutate("/settings", "PUT", settings)}
                  onLogout={logout}
                />
              )}
              <footer className="content-footer">
                <span>
                  DeltaWeave <span className="footer-divider">/</span> 내 장치
                  사이의 연결
                </span>
                <span>
                  {lastUpdated
                    ? `마지막 상태 확인 ${date(lastUpdated)}`
                    : "상태 확인 중"}{" "}
                  · 가동 {uptime(state.node.uptime_seconds)}
                </span>
              </footer>
            </>
          )}
        </main>
      </div>
      {state && dialog?.kind === "folder-form" && (
        <Modal
          title={dialog.folder ? "폴더 연결 수정" : "새 폴더 연결"}
          subtitle="내 장치의 폴더를 다른 장치와 이어 보세요."
          wide
          onClose={() => setDialog(null)}
        >
          <FolderForm
            folder={dialog.folder}
            devices={state.devices}
            defaultInterval={state.settings.poll_interval_seconds}
            browse={(path) =>
              api.request<Directory>(
                path.trim()
                  ? `/browse?path=${encodeURIComponent(path)}`
                  : "/browse",
              )
            }
            onSubmit={(input) =>
              mutate(
                dialog.folder
                  ? `/folders/${encodeURIComponent(dialog.folder.id)}`
                  : "/folders",
                dialog.folder ? "PUT" : "POST",
                input,
              )
            }
            onSaved={() =>
              saved(
                dialog.folder
                  ? "폴더 설정을 저장했습니다."
                  : "폴더 연결을 추가했습니다.",
              )
            }
          />
        </Modal>
      )}
      {dialog?.kind === "share-create" && (
        <Modal
          title="폴더 공유"
          subtitle="이 장치의 폴더를 소유자로 공유하고 안전한 키를 발급합니다."
          wide
          onClose={() => setDialog(null)}
        >
          <ShareCreateFlow
            api={api}
            browse={browse}
            onClose={() => setDialog(null)}
            onComplete={reload}
          />
        </Modal>
      )}
      {dialog?.kind === "share-join" && (
        <Modal
          title="키로 연결"
          subtitle="전달받은 키를 확인하고 이 장치의 저장 폴더를 선택합니다."
          wide
          onClose={() => setDialog(null)}
        >
          <ShareJoinFlow
            api={api}
            browse={browse}
            onClose={() => setDialog(null)}
            onComplete={reload}
          />
        </Modal>
      )}
      {dialog?.kind === "share-detail" && (
        <Modal
          title={managedDetailShare?.name ?? dialog.share.name}
          subtitle="관리형 공유 상태와 권한 관리"
          drawer
          onClose={() => setDialog(null)}
        >
          <ShareDetailFlow
            share={managedDetailShare ?? dialog.share}
            api={api}
            onClose={() => setDialog(null)}
            onRefresh={reload}
          />
          <ShareRemoveButton
            share={managedDetailShare ?? dialog.share}
            api={api}
            onRemoved={async () => {
              setDialog(null);
              await reload();
            }}
          />
        </Modal>
      )}
      {dialog?.kind === "device-form" && (
        <Modal
          title={dialog.device ? "장치 정보 수정" : "새 장치 등록"}
          subtitle="공개 연결 정보를 등록하고 폴더에서 연결하세요."
          onClose={() => setDialog(null)}
        >
          <DeviceForm
            device={dialog.device}
            onSubmit={(input) =>
              mutate(
                dialog.device
                  ? `/devices/${encodeURIComponent(dialog.device.id)}`
                  : "/devices",
                dialog.device ? "PUT" : "POST",
                input,
              )
            }
            onSaved={() =>
              saved(
                dialog.device
                  ? "장치 정보를 저장했습니다."
                  : "장치를 등록했습니다.",
              )
            }
          />
        </Modal>
      )}
      {dialog?.kind === "remove-folder" && (
        <ConfirmRemove
          name={dialog.folder.name}
          kind="folder"
          onClose={() => setDialog(null)}
          onRemove={() =>
            mutate(`/folders/${encodeURIComponent(dialog.folder.id)}`, "DELETE")
          }
        />
      )}
      {dialog?.kind === "remove-device" && (
        <ConfirmRemove
          name={dialog.device.name}
          kind="device"
          onClose={() => setDialog(null)}
          onRemove={() =>
            mutate(`/devices/${encodeURIComponent(dialog.device.id)}`, "DELETE")
          }
        />
      )}
      {folderDetail && state && (
        <Modal
          title={folderDetail.name}
          subtitle="폴더 연결 상세"
          drawer
          onClose={() => setDialog(null)}
        >
          <FolderDetail
            folder={folderDetail}
            activities={state.activities.filter(
              (a) => a.folder_id === folderDetail.id,
            )}
            onCommand={(value) => command(folderDetail, value)}
            onEdit={() =>
              setDialog({ kind: "folder-form", folder: folderDetail })
            }
            onRemove={() =>
              setDialog({ kind: "remove-folder", folder: folderDetail })
            }
          />
        </Modal>
      )}
      {dialog?.kind === "activity-detail" && (
        <Modal
          title={activityTitle(dialog.activity)}
          subtitle={date(dialog.activity.timestamp)}
          drawer
          onClose={() => setDialog(null)}
        >
          <ActivityDetail
            activity={dialog.activity}
            folder={state?.folders.find(
              (f) => f.id === dialog.activity.folder_id,
            )}
          />
        </Modal>
      )}
      {toast && (
        <div className={`toast ${toast.error ? "error" : ""}`} role="status">
          {toast.error ? <Warning size={18} /> : <CheckCircle size={18} />}
          <span>{toast.message}</span>
          <button
            className="icon-btn"
            onClick={() => setToast(null)}
            aria-label="알림 닫기"
          >
            <Check size={15} />
          </button>
        </div>
      )}
    </div>
  );
}

function PanelHeading({
  title,
  subtitle,
  count,
  action,
}: {
  title: string;
  subtitle?: string;
  count?: number;
  action?: ReactNode;
}) {
  return (
    <div className="panel-head">
      <div>
        <h2>
          {title}
          {count !== undefined && <span className="count">{count}</span>}
        </h2>
        {subtitle && <p>{subtitle}</p>}
      </div>
      {action}
    </div>
  );
}
function Overview({
  state,
  transport,
  onAdd,
  onNavigate,
  onDetail,
  onActivity,
  onCommand,
}: {
  state: AppSnapshot;
  transport: string;
  onAdd: () => void;
  onNavigate: (page: Page) => void;
  onDetail: (id: string) => void;
  onActivity: (a: Activity) => void;
  onCommand: (
    f: FolderView,
    c: "sync" | "pause" | "resume",
  ) => Promise<unknown>;
}) {
  const errors = state.folders.filter((f) => f.status === "error").length;
  const syncing = state.folders.filter((f) => f.status === "syncing").length;
  const checked = state.folders.filter((f) => f.last_sync_at !== null).length;
  const allPaused =
    state.folders.length > 0 &&
    state.folders.every((f) => ["paused", "stopped"].includes(f.status));
  const title =
    transport === "offline"
      ? "장치 상태를 다시 확인하고 있습니다"
      : !state.folders.length
        ? "첫 번째 폴더를 연결해 보세요"
        : errors
          ? `${errors}개 폴더를 확인해 주세요`
          : syncing
            ? `${syncing}개 폴더를 동기화하고 있습니다`
            : allPaused
              ? "모든 폴더가 일시정지되었습니다"
              : checked
                ? "최근 동기화 완료"
                : "폴더 연결이 준비되었습니다";
  const subtitle = !state.folders.length
    ? "내 장치에 있는 폴더를 연결하고, 파일의 흐름을 한곳에서 관리하세요."
    : errors
      ? "폴더 상세에서 오류 원인과 다음 재시도 시간을 확인할 수 있습니다."
      : allPaused
        ? "폴더에서 작업을 재개하면 다시 동기화할 수 있습니다."
        : `${checked}개 폴더에서 완료 기록 확인 · ${state.totals.active_folders}개 폴더 작업 활성화`;
  return (
    <div className="overview">
      <div className="overview-top">
        <section
          className={`connection-panel ${errors || transport === "offline" ? "status-error" : ""}`}
          aria-label="폴더 연결 개요"
        >
          <div className="connection-heading">
            <span className="connection-status">
              {errors ? (
                <Warning size={16} />
              ) : syncing ? (
                <ArrowsClockwise size={16} className="spinner" />
              ) : allPaused ? (
                <Pause size={16} />
              ) : (
                <FolderIcon size={16} />
              )}
              폴더 연결
            </span>
            <button
              className="icon-btn"
              onClick={() =>
                state.folders.length ? onNavigate("folders") : onAdd()
              }
              aria-label={
                state.folders.length ? "폴더 관리로 이동" : "첫 폴더 연결하기"
              }
            >
              <ArrowUpRight size={21} />
            </button>
          </div>
          <h2>{title}</h2>
          <p className="connection-description">{subtitle}</p>
          {state.folders.length ? (
            <div
              className="connection-map"
              role="group"
              aria-label="이 장치의 관리 폴더"
            >
              <div className="connection-origin">
                <span className="connection-node-icon">
                  <HardDrives size={30} weight="duotone" />
                </span>
                <strong>{state.node.name}</strong>
                <span>현재 장치</span>
              </div>
              <div className="connection-branches">
                {state.folders.slice(0, 3).map((folder) => (
                  <button
                    className="connection-folder"
                    key={folder.id}
                    onClick={() => onDetail(folder.id)}
                    aria-label={`${folder.name} 연결 상태 보기`}
                  >
                    <span className="connection-folder-icon">
                      <FolderIcon size={24} weight="duotone" />
                    </span>
                    <span className="connection-folder-copy">
                      <strong>{folder.name}</strong>
                      <span>
                        {folder.role === "receive"
                          ? "수신 연결"
                          : "동기화 연결"}
                      </span>
                    </span>
                    <Status status={folder.status} />
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div className="connection-start">
              <div className="connection-start-art" aria-hidden="true">
                <FolderPlus size={52} weight="duotone" />
                <Plus size={20} />
              </div>
              <button className="btn primary" onClick={onAdd}>
                첫 폴더 연결 <ArrowRight size={16} />
              </button>
            </div>
          )}
          <div className="connection-foot">
            <span>
              {state.folders.length > 3
                ? `${state.folders.length}개 중 3개 폴더 표시`
                : "이 장치에서 관리하는 폴더"}
            </span>
            <span>{state.totals.active_folders}개 작업 활성화</span>
          </div>
        </section>
        <PayloadChart history={state.history} />
      </div>
      <section className="stats" aria-label="동기화 통계">
        <Stat
          icon={<FolderIcon size={17} />}
          label="관리 폴더"
          value={number(state.totals.folders)}
          unit="개"
          foot={`${state.totals.active_folders}개 작업 활성화`}
        />
        <Stat
          icon={<File size={17} />}
          label="인덱스 파일"
          value={number(state.totals.files)}
          unit="개"
          foot="마지막으로 확인한 파일 수"
        />
        <Stat
          icon={<HardDrives size={17} />}
          label="폴더 데이터"
          value={bytes(state.totals.bytes)}
          foot="인덱스에 기록된 파일 크기"
        />
        <Stat
          icon={<ArrowsClockwise size={17} />}
          label="완료 주기 전송량"
          value={bytes(state.totals.pushed_bytes + state.totals.pulled_bytes)}
          foot={`보내기 ${bytes(state.totals.pushed_bytes)} · 받기 ${bytes(state.totals.pulled_bytes)}`}
        />
      </section>
      <section className="panel folder-section">
        <PanelHeading
          title="동기화 폴더"
          count={state.folders.length}
          action={
            <button
              className="text-button"
              onClick={() => onNavigate("folders")}
            >
              폴더 관리 <ArrowRight size={15} />
            </button>
          }
        />
        <FolderTable
          folders={state.folders.slice(0, 5)}
          devices={state.devices}
          onDetail={onDetail}
          onCommand={onCommand}
          onAdd={onAdd}
        />
        <div className="panel-foot">
          <Info size={14} />
          폴더의 일시정지는 진행 중인 작업이 끝난 뒤 적용됩니다.
        </div>
      </section>
      <div className="overview-bottom">
        <section className="panel devices-panel">
          <PanelHeading
            title="내 장치와 연결 장치"
            count={state.devices.length + 1}
            action={
              <button
                className="text-button"
                onClick={() => onNavigate("devices")}
              >
                장치 관리 <ArrowRight size={15} />
              </button>
            }
          />
          <div className="devices-mini">
            <div className="device-mini">
              <div className="item-icon local">
                <HardDrives size={22} weight="duotone" />
              </div>
              <div>
                <strong>{state.node.name}</strong>
                <p>{state.node.platform} · 이 장치</p>
              </div>
              <span className="device-local-label">관리 중</span>
            </div>
            {state.devices.slice(0, 3).map((device) => (
              <div className="device-mini" key={device.id}>
                <div className="item-icon">
                  <Desktop size={22} weight="duotone" />
                </div>
                <div>
                  <strong>{device.name}</strong>
                  <p>
                    {device.last_seen_at
                      ? `최근 응답 ${ago(device.last_seen_at)}`
                      : "아직 응답 기록 없음"}
                  </p>
                </div>
                <span className="device-registered-label">등록됨</span>
              </div>
            ))}
          </div>
          {!state.devices.length && (
            <Empty
              compact
              title="연결 장치를 추가하세요"
              description="상대 장치의 공개 연결 정보를 등록하면 폴더를 연결할 때 선택할 수 있습니다."
              icon={<Desktop size={23} />}
            />
          )}
          <div className="panel-foot">
            <Desktop size={14} />
            응답 기록은 마지막으로 성공한 요청을 기준으로 합니다.
          </div>
        </section>
        <section className="panel activity-panel">
          <PanelHeading
            title="최근 활동"
            action={
              <button
                className="text-button"
                onClick={() => onNavigate("activity")}
              >
                모든 활동 <ArrowRight size={15} />
              </button>
            }
          />
          <ActivityList
            activities={[...state.activities]
              .sort((a, b) => b.timestamp - a.timestamp)
              .slice(0, 4)}
            onDetail={onActivity}
          />
        </section>
      </div>
      {state.totals.conflicts > 0 && (
        <div className="notice warning conflict-notice">
          <Warning size={18} />
          <span>
            보존된 활동 기록에 충돌 {number(state.totals.conflicts)}건이
            있습니다. 활동에서 충돌 사본 경로를 확인하세요.
          </span>
          <button
            className="text-button"
            onClick={() => onNavigate("activity")}
          >
            활동 확인
          </button>
        </div>
      )}
    </div>
  );
}
function Stat({
  icon,
  label,
  value,
  unit,
  foot,
}: {
  icon: ReactNode;
  label: string;
  value: string;
  unit?: string;
  foot: string;
}) {
  return (
    <div className="stat">
      <div className="stat-label">
        {icon}
        {label}
      </div>
      <div className="stat-value">
        {value}
        {unit && <small>{unit}</small>}
      </div>
      <div className="stat-foot">{foot}</div>
    </div>
  );
}

function PayloadChart({ history }: { history: HistoryPoint[] }) {
  const [limit, setLimit] = useState(24);
  const [hover, setHover] = useState<number | null>(null);
  const points = [...history]
    .sort((a, b) => a.timestamp - b.timestamp)
    .slice(-limit);
  const max = Math.max(
    1,
    ...points.flatMap((p) => [p.pushed_bytes, p.pulled_bytes]),
  );
  const width = 660;
  const height = 156;
  const x = (i: number) =>
    points.length === 1 ? width / 2 : (i * width) / (points.length - 1);
  const y = (v: number) => height - 8 - (v / max) * (height - 18);
  const path = (key: "pushed_bytes" | "pulled_bytes") =>
    points
      .map(
        (p, i) => `${i ? "L" : "M"}${x(i).toFixed(2)},${y(p[key]).toFixed(2)}`,
      )
      .join(" ");
  const pushed = points.reduce((n, p) => n + p.pushed_bytes, 0);
  const pulled = points.reduce((n, p) => n + p.pulled_bytes, 0);
  return (
    <section className="panel chart-panel">
      <PanelHeading
        title="동기화 전송량"
        subtitle="완료된 동기화 주기의 실제 전송량"
        action={
          <select
            className="select-compact"
            aria-label="그래프에 표시할 완료 주기 수"
            value={limit}
            onChange={(e) => {
              setLimit(Number(e.target.value));
              setHover(null);
            }}
          >
            <option value="24">최근 24주기</option>
            <option value="60">최근 60주기</option>
            <option value="120">최근 120주기</option>
          </select>
        }
      />
      <div className="chart-top">
        <div>
          <div className="legend-label">
            <span className="legend-dot" />
            보내기
            <ArrowUpRight size={11} />
          </div>
          <div className="chart-number">{bytes(pushed)}</div>
        </div>
        <div>
          <div className="legend-label">
            <span className="legend-dot mint" />
            받기
            <ArrowDownLeft size={11} />
          </div>
          <div className="chart-number">{bytes(pulled)}</div>
        </div>
      </div>
      <div className="chart-wrap">
        {points.length > 0 && (
          <span className="chart-scale">
            {bytes(max === 1 && pushed + pulled === 0 ? 0 : max)}
          </span>
        )}
        <svg
          viewBox={`0 0 ${width} ${height}`}
          preserveAspectRatio="none"
          role="group"
          aria-label={`최근 ${points.length}개 완료 주기: 보내기 ${bytes(pushed)}, 받기 ${bytes(pulled)}`}
        >
          {[8, 54, 100, 146].map((v) => (
            <line
              key={v}
              x1="0"
              y1={v}
              x2={width}
              y2={v}
              stroke="var(--chart-grid, #dce3db)"
              strokeDasharray="3 5"
              strokeWidth=".8"
            />
          ))}
          {points.length > 0 && (
            <>
              <path
                d={`${path("pushed_bytes")} L${x(points.length - 1)},${height} L${x(0)},${height} Z`}
                fill="var(--chart-area, #e9eee7)"
              />
              <path
                d={path("pushed_bytes")}
                fill="none"
                stroke="var(--chart-push, #235c43)"
                strokeWidth="2"
                vectorEffect="non-scaling-stroke"
              />
              <path
                d={path("pulled_bytes")}
                fill="none"
                stroke="var(--chart-pull, #81986f)"
                strokeWidth="2"
                vectorEffect="non-scaling-stroke"
              />
              {points.map((p, i) => (
                <g key={`${p.folder_id}-${p.timestamp}-${i}`}>
                  <circle
                    cx={x(i)}
                    cy={y(p.pushed_bytes)}
                    r={points.length < 3 ? 3 : 1.7}
                    fill="var(--chart-push, #235c43)"
                  />
                  <circle
                    cx={x(i)}
                    cy={y(p.pulled_bytes)}
                    r={points.length < 3 ? 3 : 1.7}
                    fill="var(--chart-pull, #81986f)"
                  />
                  <rect
                    className="graph-hit"
                    x={Math.max(
                      0,
                      x(i) - width / Math.max(2, points.length) / 2,
                    )}
                    y="0"
                    width={width / Math.max(1, points.length - 1)}
                    height={height}
                    tabIndex={0}
                    role="button"
                    aria-label={`${date(p.timestamp)} 보내기 ${bytes(p.pushed_bytes)} 받기 ${bytes(p.pulled_bytes)}`}
                    onMouseEnter={() => setHover(i)}
                    onMouseLeave={() => setHover(null)}
                    onFocus={() => setHover(i)}
                    onBlur={() => setHover(null)}
                  />
                </g>
              ))}
            </>
          )}
        </svg>
        <div className="chart-axis">
          {points.length ? (
            <>
              <span>{time(points[0].timestamp)}</span>
              <span>{points.length}개 완료 주기</span>
              <span>{time(points[points.length - 1].timestamp)}</span>
            </>
          ) : (
            <>
              <span>완료 기록 대기</span>
              <span>동기화 주기별</span>
            </>
          )}
        </div>
        {!points.length && (
          <div className="chart-empty">
            <ChartLineUp size={23} />
            <span>첫 동기화가 끝나면 그래프가 시작됩니다.</span>
            <small>전송량은 실제 완료 주기에서 집계합니다.</small>
          </div>
        )}
        {hover !== null && points[hover] && (
          <div className="graph-tooltip">
            {date(points[hover].timestamp)} · ↑{" "}
            {bytes(points[hover].pushed_bytes)} · ↓{" "}
            {bytes(points[hover].pulled_bytes)}
          </div>
        )}
      </div>
      <div className="chart-foot">
        <span className="chart-idle-dot" />
        파일 변경이 없는 주기는 0 B로 표시됩니다.
      </div>
    </section>
  );
}

function FolderTable({
  folders,
  devices,
  onDetail,
  onCommand,
  onAdd,
}: {
  folders: FolderView[];
  devices: DeviceView[];
  onDetail: (id: string) => void;
  onCommand: (
    f: FolderView,
    c: "sync" | "pause" | "resume",
  ) => Promise<unknown>;
  onAdd?: () => void;
}) {
  if (!folders.length)
    return (
      <Empty
        title={
          onAdd ? "아직 연결된 폴더가 없습니다" : "조건에 맞는 폴더가 없습니다"
        }
        description={
          onAdd
            ? "동기화할 로컬 폴더와 상대 장치를 지정하세요. 기존 상태와 identity 파일도 가져올 수 있습니다."
            : "검색어나 상태 필터를 바꿔 보세요."
        }
        action={
          onAdd ? (
            <button className="btn subtle small" onClick={onAdd}>
              <Plus size={14} />첫 폴더 연결
            </button>
          ) : null
        }
      />
    );
  return (
    <>
      <div className="table-head" aria-hidden="true">
        <span>폴더 이름 / 경로</span>
        <span>연결 대상</span>
        <span>상태</span>
        <span>최근 동기화</span>
        <span>작업</span>
      </div>
      <div className="folder-table">
        {folders.map((folder) => (
          <div className="folder-row" key={folder.id}>
            <button
              className="folder-main"
              onClick={() => onDetail(folder.id)}
              aria-label={`${folder.name} 상세 보기`}
            >
              <span className="item-icon">
                <FolderIcon size={21} weight="duotone" />
              </span>
              <span className="folder-name">
                <strong>{folder.name}</strong>
                <span className="path mono" title={folder.root}>
                  {folder.root}
                </span>
                <span className="folder-info">
                  {number(folder.files_count)}개 파일 <span>·</span>{" "}
                  {bytes(folder.total_bytes)}
                </span>
              </span>
            </button>
            <div className="folder-peer">
              {devices.find((d) => d.id === folder.device_id)?.name ??
                (folder.role === "receive"
                  ? "상대 연결 수신"
                  : (folder.peer_endpoint_id?.slice(0, 12) ??
                    "연결 설정 필요"))}
              <small>
                {folder.role === "receive"
                  ? "허용 장치 " + (folder.allowed_peers?.length ?? 0) + "개"
                  : "양방향 폴더 연결"}
              </small>
            </div>
            <div className="status-wrap">
              <Status status={folder.status} />
            </div>
            <div className="folder-time" title={date(folder.last_sync_at)}>
              {ago(folder.last_sync_at)}
            </div>
            <FolderControls
              folder={folder}
              onCommand={(c) => onCommand(folder, c)}
            />
          </div>
        ))}
      </div>
    </>
  );
}
function FoldersPage({
  state,
  onAdd,
  onDetail,
  onCommand,
}: {
  state: AppSnapshot;
  onAdd: () => void;
  onDetail: (id: string) => void;
  onCommand: (
    f: FolderView,
    c: "sync" | "pause" | "resume",
  ) => Promise<unknown>;
}) {
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState("all");
  const folders = state.folders.filter(
    (f) =>
      (f.name + " " + f.root).toLowerCase().includes(query.toLowerCase()) &&
      (status === "all" || f.status === status),
  );
  return (
    <>
      <div className="toolbar">
        <div className="search">
          <MagnifyingGlass size={16} />
          <input
            aria-label="폴더 검색"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="폴더 이름 또는 경로 검색"
          />
        </div>
        <select
          aria-label="폴더 상태 필터"
          value={status}
          onChange={(e) => setStatus(e.target.value)}
        >
          <option value="all">모든 상태</option>
          <option value="idle">주기 대기</option>
          <option value="syncing">동기화 중</option>
          <option value="listening">수신 대기</option>
          <option value="paused">일시정지</option>
          <option value="pausing">일시정지 대기</option>
          <option value="starting">시작 중</option>
          <option value="stopped">중지됨</option>
          <option value="error">확인 필요</option>
        </select>
      </div>
      <section className="panel">
        <PanelHeading
          title="내 폴더"
          count={folders.length}
          subtitle="각 폴더는 독립된 연결과 작업 상태를 갖습니다."
        />
        <FolderTable
          folders={folders}
          devices={state.devices}
          onAdd={!state.folders.length ? onAdd : undefined}
          onDetail={onDetail}
          onCommand={onCommand}
        />
        <div className="panel-foot">
          <ShieldCheck size={13} />
          연결을 제거해도 동기화된 파일은 보존됩니다.
        </div>
      </section>
    </>
  );
}

function DevicesPage({
  state,
  onAdd,
  onEdit,
  onRemove,
}: {
  state: AppSnapshot;
  onAdd: () => void;
  onEdit: (d: DeviceView) => void;
  onRemove: (d: DeviceView) => void;
}) {
  const [query, setQuery] = useState("");
  const devices = state.devices.filter((d) =>
    (d.name + " " + d.endpoint_id + " " + d.address)
      .toLowerCase()
      .includes(query.toLowerCase()),
  );
  return (
    <>
      <div className="notice">
        <Info size={18} />
        <span>
          장치의 최근 응답 시간은 성공한 요청을 기준으로 합니다. 등록된 장치의
          현재 연결 상태를 보장하지 않습니다. 접근 허용은 수신 폴더의 허용 장치
          목록에서 설정하세요.
        </span>
      </div>
      <div className="toolbar">
        <div className="search">
          <MagnifyingGlass size={16} />
          <input
            aria-label="장치 검색"
            placeholder="장치 이름, ID 또는 주소 검색"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
      </div>
      {devices.length ? (
        <div className="device-grid">
          {devices.map((device) => {
            const connections = state.folders.filter(
              (f) =>
                f.device_id === device.id ||
                f.peer_endpoint_id === device.endpoint_id ||
                f.allowed_peers?.includes(device.endpoint_id),
            );
            return (
              <article className="panel device-card" key={device.id}>
                <div className="device-card-top">
                  <div className="item-icon">
                    <Desktop size={22} />
                  </div>
                  <div>
                    <h2>{device.name}</h2>
                    <p>
                      {device.last_seen_at
                        ? `최근 응답 ${ago(device.last_seen_at)}`
                        : "아직 성공 응답 기록 없음"}
                    </p>
                  </div>
                  <button
                    className="icon-btn"
                    aria-label={`${device.name} 수정`}
                    onClick={() => onEdit(device)}
                  >
                    <PencilSimple size={17} />
                  </button>
                </div>
                <dl className="device-facts">
                  <div>
                    <dt>공개 endpoint ID</dt>
                    <dd className="mono">{device.endpoint_id}</dd>
                  </div>
                  <div>
                    <dt>직접 연결 주소</dt>
                    <dd className="mono">{device.address}</dd>
                  </div>
                  <div>
                    <dt>이 장치와 연결한 폴더</dt>
                    <dd>
                      {connections.length
                        ? connections.map((f) => f.name).join(", ")
                        : "아직 연결한 폴더 없음"}
                    </dd>
                  </div>
                  <div>
                    <dt>마지막 성공 응답</dt>
                    <dd>{date(device.last_seen_at)}</dd>
                  </div>
                </dl>
                <div className="device-card-bottom">
                  <CopyButton
                    value={publicConnection(device)}
                    label="연결 정보 복사"
                  />
                  <button
                    className="icon-btn"
                    aria-label={`${device.name} 제거`}
                    title="장치 제거"
                    onClick={() => onRemove(device)}
                  >
                    <Trash size={16} />
                  </button>
                </div>
              </article>
            );
          })}
        </div>
      ) : (
        <section className="panel">
          <Empty
            title={
              state.devices.length
                ? "검색 결과가 없습니다"
                : "다른 장치와 연결해 보세요"
            }
            description={
              state.devices.length
                ? "장치 이름이나 공개 ID로 다시 검색하세요."
                : "상대 장치에서 복사한 공개 연결 정보로 장치를 등록하세요. 폴더를 연결하면 실제 동기화 결과를 확인할 수 있습니다."
            }
            icon={<Desktop size={26} />}
            action={
              !state.devices.length ? (
                <button className="btn primary" onClick={onAdd}>
                  <Plus size={16} />첫 장치 등록
                </button>
              ) : null
            }
          />
        </section>
      )}
    </>
  );
}

function ActivityList({
  activities,
  onDetail,
}: {
  activities: Activity[];
  onDetail: (a: Activity) => void;
}) {
  if (!activities.length)
    return (
      <Empty
        compact
        title="아직 활동 기록이 없습니다"
        description="폴더 설정과 동기화 작업이 시작되면 실제 활동이 여기에 표시됩니다."
        icon={<Pulse size={24} />}
      />
    );
  return (
    <div className="activity-list">
      {activities.map((activity) => (
        <button
          className="activity-row"
          key={activity.id}
          onClick={() => onDetail(activity)}
        >
          <span
            className={`activity-symbol ${/error|conflict|fail/.test(activity.kind) ? "error" : ""}`}
          >
            {/error|conflict|fail/.test(activity.kind) ? (
              <Warning size={15} />
            ) : activity.pushed_bytes + activity.pulled_bytes > 0 ? (
              <ArrowsClockwise size={14} />
            ) : (
              <Check size={14} />
            )}
          </span>
          <span className="activity-copy">
            <strong>{activityTitle(activity)}</strong>
            <p>{activityDetail(activity)}</p>
          </span>
          <time dateTime={new Date(activity.timestamp).toISOString()}>
            {time(activity.timestamp)}
          </time>
          <CaretRight size={12} className="muted activity-chevron" />
        </button>
      ))}
    </div>
  );
}
function ActivityPage({
  state,
  api,
  onDetail,
}: {
  state: AppSnapshot;
  api: Api;
  onDetail: (a: Activity) => void;
}) {
  const [query, setQuery] = useState("");
  const [folder, setFolder] = useState("all");
  const [kind, setKind] = useState("all");
  const [activityPage, setActivityPage] = useState(1);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState("");
  const kinds = [...new Set(state.activities.map((a) => a.kind))].sort();
  const activities = [...state.activities]
    .filter(
      (a) =>
        (folder === "all" || a.folder_id === folder) &&
        (kind === "all" || a.kind === kind) &&
        (
          activityTitle(a) +
          " " +
          activityDetail(a) +
          " " +
          a.title +
          " " +
          a.detail +
          " " +
          (a.path ?? "")
        )
          .toLowerCase()
          .includes(query.toLowerCase()),
    )
    .sort((a, b) => b.timestamp - a.timestamp);
  const pageSize = 25;
  const pageCount = Math.max(1, Math.ceil(activities.length / pageSize));
  const currentPage = Math.min(activityPage, pageCount);
  const firstIndex = (currentPage - 1) * pageSize;
  const visibleActivities = activities.slice(firstIndex, firstIndex + pageSize);
  useEffect(() => {
    setActivityPage((previous) => Math.min(previous, pageCount));
  }, [pageCount]);
  async function exportHistory() {
    setExporting(true);
    setError("");
    try {
      const data = await api.request("/activities/export");
      const blob = new Blob([JSON.stringify(data, null, 2)], {
        type: "application/json",
      });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `deltaweave-activities-${new Date().toISOString().slice(0, 10)}.json`;
      document.body.append(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setExporting(false);
    }
  }
  return (
    <>
      <div className="toolbar activity-filters">
        <div className="search">
          <MagnifyingGlass size={16} />
          <input
            aria-label="활동 검색"
            placeholder="활동 내용 또는 파일 경로 검색"
            value={query}
            onChange={(e) => {
              setQuery(e.target.value);
              setActivityPage(1);
            }}
          />
        </div>
        <select
          aria-label="활동 폴더 필터"
          value={folder}
          onChange={(e) => {
            setFolder(e.target.value);
            setActivityPage(1);
          }}
        >
          <option value="all">모든 폴더</option>
          {state.folders.map((f) => (
            <option key={f.id} value={f.id}>
              {f.name}
            </option>
          ))}
        </select>
        <select
          aria-label="활동 유형 필터"
          value={kind}
          onChange={(e) => {
            setKind(e.target.value);
            setActivityPage(1);
          }}
        >
          <option value="all">모든 활동</option>
          {kinds.map((k) => (
            <option key={k} value={k}>
              {activityKind(k)}
            </option>
          ))}
        </select>
        <button
          className="btn subtle"
          onClick={exportHistory}
          disabled={exporting}
        >
          <DownloadSimple size={16} />
          {exporting ? "내보내는 중…" : "기록 내보내기"}
        </button>
      </div>
      <ErrorBox error={error} />
      <section className="panel full-activities">
        <PanelHeading
          title="활동 기록"
          count={activities.length}
          subtitle={`최근 ${number(state.settings.history_limit)}건까지 보존 · 내보내기는 보존된 전체 기록을 포함합니다.`}
        />
        {activities.length ? (
          <>
            <ActivityList activities={visibleActivities} onDetail={onDetail} />
            <nav className="activity-pagination" aria-label="활동 페이지">
              <span className="pagination-count" role="status">
                {number(firstIndex + 1)}–
                {number(firstIndex + visibleActivities.length)} /{" "}
                {number(activities.length)}건
              </span>
              <div className="pagination-controls">
                <button
                  className="btn subtle small"
                  aria-label="이전 활동 페이지"
                  disabled={currentPage === 1}
                  onClick={() => setActivityPage(currentPage - 1)}
                >
                  이전
                </button>
                <span>
                  {number(currentPage)} / {number(pageCount)}
                </span>
                <button
                  className="btn subtle small"
                  aria-label="다음 활동 페이지"
                  disabled={currentPage === pageCount}
                  onClick={() => setActivityPage(currentPage + 1)}
                >
                  다음
                </button>
              </div>
            </nav>
          </>
        ) : (
          <Empty
            title={
              state.activities.length
                ? "조건에 맞는 활동이 없습니다"
                : "아직 활동 기록이 없습니다"
            }
            description={
              state.activities.length
                ? "검색어나 폴더, 활동 유형 필터를 바꿔 보세요."
                : "설정 변경과 파일 동기화가 진행되면 실제 결과가 여기에 기록됩니다."
            }
            icon={<ActivityIcon size={25} />}
          />
        )}
      </section>
    </>
  );
}
function activityKind(kind: string) {
  return (
    (
      {
        sync: "동기화",
        sync_complete: "동기화 완료",
        complete: "동기화 완료",
        file_received: "파일 받음",
        file_sent: "파일 보냄",
        peer_seen: "상대 장치 응답",
        pulling: "변경 받기",
        pushing: "변경 보내기",
        error: "오류",
        conflict: "충돌",
        conflict_preserved: "충돌 사본 보존",
        conflict_decision: "충돌 판정",
        folder_added: "폴더 추가",
        folder_removed: "폴더 제거",
        folder_updated: "폴더 수정",
        device_added: "장치 추가",
        settings_updated: "설정 변경",
        paused: "일시정지",
        resumed: "재개",
        file: "파일 작업",
        phase: "작업 단계",
      } as Record<string, string>
    )[kind] ?? kind
  );
}

function SettingsPage({
  state,
  onSave,
  onLogout,
}: {
  state: AppSnapshot;
  onSave: (s: Settings) => Promise<unknown>;
  onLogout: () => void;
}) {
  const [form, setForm] = useState<Settings>(state.settings);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState("");
  const [saved, setSaved] = useState(false);
  async function submit(e: FormEvent) {
    e.preventDefault();
    if (pending) return;
    setPending(true);
    setError("");
    setSaved(false);
    try {
      await onSave({ ...form, node_name: form.node_name.trim() });
      setSaved(true);
    } catch (e) {
      setError(errorMessage(e));
    } finally {
      setPending(false);
    }
  }
  return (
    <div className="settings-layout">
      <section className="panel">
        <PanelHeading
          title="기본 설정"
          subtitle="이 관리 장치에 적용할 설정입니다."
        />
        <form className="settings-form" onSubmit={submit}>
          <fieldset disabled={pending} className="form-fields">
            <Field
              label="장치 표시 이름"
              help="현재 관리 중인 장치를 구분하기 쉽게 이름을 정하세요."
            >
              <input
                required
                maxLength={100}
                value={form.node_name}
                onChange={(e) => {
                  setForm({ ...form, node_name: e.target.value });
                  setSaved(false);
                }}
              />
            </Field>
            <div className="form-grid">
              <Field
                label="기본 동기화 간격 (초)"
                help="새 폴더를 만들 때 사용할 기본 간격입니다."
              >
                <input
                  type="number"
                  min="1"
                  max="86400"
                  required
                  value={form.poll_interval_seconds}
                  onChange={(e) => {
                    setForm({
                      ...form,
                      poll_interval_seconds: Number(e.target.value),
                    });
                    setSaved(false);
                  }}
                />
              </Field>
              <Field
                label="활동 기록 보존 수"
                help="오래된 기록부터 보존 범위를 조정합니다."
              >
                <input
                  type="number"
                  min="1"
                  max="10000"
                  required
                  value={form.history_limit}
                  onChange={(e) => {
                    setForm({ ...form, history_limit: Number(e.target.value) });
                    setSaved(false);
                  }}
                />
              </Field>
            </div>
          </fieldset>
          <ErrorBox error={error} />
          <div className="form-actions">
            {saved && (
              <span className="form-success" role="status">
                <CheckCircle size={15} />
                설정을 저장했습니다.
              </span>
            )}
            <button className="btn primary" disabled={pending}>
              {pending ? "저장 중…" : "변경 사항 저장"}
              <Check size={16} />
            </button>
          </div>
        </form>
      </section>
      <div>
        <section className="panel settings-note">
          <h3>현재 관리 접속</h3>
          <p>브라우저 관리 인증은 장치 간 연결 인증과 별도로 관리합니다.</p>
          <dl className="details">
            <div>
              <dt>관리 주소</dt>
              <dd className="mono">{location.origin}</dd>
            </div>
            <div>
              <dt>실행 장치</dt>
              <dd>
                {state.node.name} · {state.node.platform}
              </dd>
            </div>
            <div>
              <dt>버전</dt>
              <dd className="mono">{state.node.version}</dd>
            </div>
            <div>
              <dt>서비스 시작</dt>
              <dd>{date(state.node.started_at)}</dd>
            </div>
          </dl>
          <div className="form-actions">
            <button className="btn subtle small" onClick={onLogout}>
              <SignOut size={15} />이 브라우저에서 로그아웃
            </button>
          </div>
        </section>
        <section className="panel settings-note wide-panel">
          <h3>폴더마다 세밀하게</h3>
          <p>
            동기화 간격, 수신 연결 수, 최소 디스크 여유 공간은 폴더 연결의 고급
            설정에서 조정할 수 있습니다.
          </p>
          <p>
            관리 접속 주소와 접근 키 변경은 이 장치의 시작 설정에서 적용합니다.
          </p>
        </section>
      </div>
    </div>
  );
}

function FolderDetail({
  folder,
  activities,
  onCommand,
  onEdit,
  onRemove,
}: {
  folder: FolderView;
  activities: Activity[];
  onCommand: (c: "sync" | "pause" | "resume") => Promise<unknown>;
  onEdit: () => void;
  onRemove: () => void;
}) {
  const conflicts = activities.filter(
    (a) => a.kind === "conflict_preserved" && a.path,
  );
  return (
    <>
      <div className="detail-heading">
        <div className="item-icon">
          <FolderIcon size={25} />
        </div>
        <div>
          <h3>
            {folder.role === "sync" ? "상대 폴더에 연결" : "연결 요청 수신"}
          </h3>
          <p className="muted detail-subtitle">양방향 폴더 동기화</p>
        </div>
        <Status status={folder.status} />
      </div>
      {folder.last_error && (
        <div className="notice error detail-notice">
          <Warning size={17} />
          <span>
            {folder.last_error}
            {folder.retry_at && (
              <small className="notice-timestamp">
                다음 재시도 {date(folder.retry_at)}
              </small>
            )}
          </span>
        </div>
      )}
      {folder.status === "pausing" && (
        <div className="notice warning detail-notice">
          <Clock size={17} />
          <span>
            현재 작업이 끝난 뒤 일시정지됩니다. 완료될 때까지 기다려 주세요.
          </span>
        </div>
      )}
      <dl className="details">
        <div>
          <dt>로컬 폴더 경로</dt>
          <dd className="mono">{folder.root}</dd>
        </div>
        <div className="details-grid">
          <div>
            <dt>인덱스 파일 수</dt>
            <dd>{number(folder.files_count)}개</dd>
          </div>
          <div>
            <dt>파일 크기</dt>
            <dd className="mono">{bytes(folder.total_bytes)}</dd>
          </div>
        </div>
        <div>
          <dt>최근 동기화</dt>
          <dd>{date(folder.last_sync_at)}</dd>
        </div>
        {folder.phase && (
          <div>
            <dt>현재 작업 단계</dt>
            <dd>{phaseLabel(folder.phase)}</dd>
          </div>
        )}
        {folder.current_path && (
          <div>
            <dt>처리 중인 파일</dt>
            <dd className="mono">{folder.current_path}</dd>
          </div>
        )}
        <div className="details-grid">
          <div>
            <dt>동기화 간격</dt>
            <dd>{folder.interval_seconds}초</dd>
          </div>
          <div>
            <dt>최소 여유 공간</dt>
            <dd>{folder.min_free_space_mib} MiB</dd>
          </div>
        </div>
        {folder.role === "sync" && (
          <>
            <div>
              <dt>상대 공개 endpoint ID</dt>
              <dd className="mono">{folder.peer_endpoint_id || "미설정"}</dd>
            </div>
            <div>
              <dt>상대 직접 연결 주소</dt>
              <dd className="mono">
                {folder.direct_addresses?.join("\n") || "직접 주소 없음"}
              </dd>
            </div>
          </>
        )}
        {folder.role === "receive" && (
          <div>
            <dt>허용한 상대 공개 endpoint ID</dt>
            <dd className="mono">
              {folder.allowed_peers?.join("\n") || "아직 허용한 장치 없음"}
            </dd>
          </div>
        )}
      </dl>
      <div className="inline-commands">
        <FolderControls folder={folder} onCommand={onCommand} />
        <button className="btn subtle small" onClick={onEdit}>
          <PencilSimple size={14} />
          설정 수정
        </button>
      </div>
      <div className="form-section detail-section">
        <h3>이 폴더의 공개 연결 정보</h3>
        <p className="muted detail-description">
          상대 장치에서 이 정보를 가져오세요. 수신 폴더에는 상대 장치의 공개
          ID도 허용해야 합니다.
        </p>
        <pre className="connection-code mono">{publicConnection(folder)}</pre>
        <div className="copy-row">
          <CopyButton value={publicConnection(folder)} />
        </div>
      </div>
      {folder.last_report && (
        <details className="advanced detail-section">
          <summary>마지막 동기화 결과</summary>
          <pre className="connection-code mono">
            {JSON.stringify(folder.last_report, null, 2)}
          </pre>
        </details>
      )}
      <details className="advanced">
        <summary>상태와 identity 경로</summary>
        <dl className="details">
          <div>
            <dt>상태 저장 경로</dt>
            <dd className="mono">{folder.state_path}</dd>
          </div>
          <div>
            <dt>identity 파일 경로</dt>
            <dd className="mono">{folder.identity_path}</dd>
          </div>
        </dl>
      </details>
      <div className="form-section detail-section">
        <h3>보존된 충돌 사본</h3>
        {conflicts.length ? (
          conflicts.map((a) => (
            <div className="notice warning" key={a.id}>
              <Warning size={16} />
              <div>
                {a.detail}
                <p className="mono conflict-path">
                  {a.path || "별도 사본 경로가 기록되지 않았습니다."}
                </p>
                <small>{date(a.timestamp)}</small>
              </div>
            </div>
          ))
        ) : (
          <p className="muted detail-description">
            보존된 활동 기록에 충돌 사본이 없습니다.
          </p>
        )}
      </div>
      <div className="danger-zone">
        <button className="btn danger small" onClick={onRemove}>
          <Trash size={15} />
          폴더 연결 제거
        </button>
        <p className="permission-note">
          <Info size={13} />
          실제 폴더와 동기화된 파일은 삭제하지 않습니다.
        </p>
      </div>
    </>
  );
}
function activityTitle(activity: Activity) {
  return activity.title === activity.kind
    ? activityKind(activity.kind)
    : activity.title;
}
function activityDetail(activity: Activity) {
  const match = /^(\d+) local, (\d+) remote actions$/.exec(activity.detail);
  if (match)
    return `이 장치 ${number(Number(match[1]))}건 · 상대 장치 ${number(Number(match[2]))}건 변경 적용`;
  return activity.detail || activity.path || activityKind(activity.kind);
}
function phaseLabel(phase: string) {
  return (
    (
      {
        scanning: "파일 스캔",
        comparing: "변경 비교",
        receiving: "파일 수신",
        applying: "변경 적용",
        verifying: "최종 검증",
        connecting: "상대 장치 연결",
        idle: "주기 대기",
        syncing: "동기화",
        complete: "동기화 완료",
        pulling: "변경 받기",
        pushing: "변경 보내기",
        peer_seen: "상대 장치 응답 확인",
        file_received: "파일 받음",
        file_sent: "파일 보냄",
      } as Record<string, string>
    )[phase] ?? phase
  );
}
function ActivityDetail({
  activity,
  folder,
}: {
  activity: Activity;
  folder?: FolderView;
}) {
  return (
    <>
      <div
        className={`notice ${/error|conflict|fail/.test(activity.kind) ? "warning" : ""}`}
      >
        <ActivityIcon size={18} />
        <span>{activityDetail(activity)}</span>
      </div>
      <dl className="details">
        <div>
          <dt>활동 유형</dt>
          <dd>{activityKind(activity.kind)}</dd>
        </div>
        <div>
          <dt>폴더</dt>
          <dd>
            {folder?.name ?? (activity.folder_id ? "제거된 폴더" : "장치 전체")}
          </dd>
        </div>
        <div>
          <dt>발생 시각</dt>
          <dd>{date(activity.timestamp)}</dd>
        </div>
        {activity.path && (
          <div>
            <dt>
              {activity.kind === "conflict_preserved" && activity.path
                ? "보존된 충돌 사본 경로"
                : "파일 경로"}
            </dt>
            <dd className="mono">{activity.path}</dd>
          </div>
        )}
        <div className="details-grid">
          <div>
            <dt>보낸 payload</dt>
            <dd className="mono">{bytes(activity.pushed_bytes)}</dd>
          </div>
          <div>
            <dt>받은 payload</dt>
            <dd className="mono">{bytes(activity.pulled_bytes)}</dd>
          </div>
        </div>
      </dl>
      {activity.kind === "conflict_preserved" && activity.path && (
        <p className="permission-note">
          <ShieldCheck size={14} />
          충돌 사본은 자동 삭제하지 않습니다. 표시된 파일을 확인하세요.
        </p>
      )}
    </>
  );
}
