import { availableActions, formatBytes, formatCount, phaseCopy, recordStatus, validateConnection, verifiedSync } from './model.js';

const byId = id => document.getElementById(id);
const text = (id, value) => {
  const node = byId(id);
  if (node && node.textContent !== String(value)) node.textContent = String(value);
};
const make = (tag, value, className) => {
  const node = document.createElement(tag);
  if (value !== undefined) node.textContent = String(value);
  if (className) node.className = className;
  return node;
};

let token = '';
let connected = false;
let pending = false;
let state = null;
let pollTimer;
let requestEpoch = 0;
let scanSignature = '';
let syncSignature = '';
let activitySignature = '';
let announcedActivity = '';
let reportSignature = '';

function announce(message) { text('announcement', message); }

function bootstrapToken() {
  const fragment = new URLSearchParams(location.hash.slice(1));
  if (fragment.has('token')) {
    token = fragment.get('token') ?? '';
    history.replaceState(null, '', location.pathname + location.search);
  } else {
    try { token = sessionStorage.getItem('deltaweave.session') ?? ''; } catch { /* Storage can be disabled. */ }
  }
}

function saveToken() {
  try { sessionStorage.setItem('deltaweave.session', token); } catch { /* Current-page access still works. */ }
}

function showError(id, message) {
  const target = byId(`${id}-error`);
  if (target) {
    target.textContent = message ?? '';
    target.hidden = !message;
  }
  const input = byId(id);
  if (input) {
    if (message) input.setAttribute('aria-invalid', 'true');
    else input.removeAttribute('aria-invalid');
  }
}

function formMessage(message) {
  text('form-error', message ?? '');
  byId('form-error').hidden = !message;
}

function connectionMessage(message) {
  text('connection-message', message ?? '');
  byId('connection-banner').hidden = !message;
}

async function request(path, body) {
  const controller = new AbortController();
  const deadline = setTimeout(() => controller.abort(), 12000);
  try {
    const response = await fetch(path, {
      method: body === undefined ? 'GET' : 'POST',
      headers: { Authorization: `Bearer ${token}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
      body: body === undefined ? undefined : JSON.stringify(body),
      cache: 'no-store',
      credentials: 'omit',
      signal: controller.signal,
    });
    let data;
    try { data = await response.json(); } catch { data = { error: '서버 응답을 읽을 수 없습니다.' }; }
    if (!response.ok) {
      const error = new Error(data.error || '요청을 처리하지 못했습니다.');
      error.status = response.status;
      error.field = data.field;
      throw error;
    }
    if (!data || typeof data.phase !== 'string' || !Array.isArray(data.activity)) {
      throw new Error('서버 상태 응답이 올바르지 않습니다.');
    }
    return data;
  } finally {
    clearTimeout(deadline);
  }
}

function disconnected(error) {
  connected = false;
  clearTimeout(pollTimer);
  if (error.status === 401 || error.status === 403) {
    byId('auth-panel').hidden = false;
    showError('token', '접속 키를 확인하세요. 서버를 다시 시작했다면 새 접속 링크가 필요합니다.');
    byId('token').focus();
    try { sessionStorage.removeItem('deltaweave.session'); } catch { /* No stored credential to remove. */ }
    connectionMessage(state ? '접속 권한을 확인할 수 없습니다. 아래 결과는 마지막으로 받은 정보입니다.' : '');
  } else {
    const message = '서버에 연결할 수 없습니다. 실행 상태를 확인하고 다시 연결하세요.';
    connectionMessage(state ? `${message} 표시된 결과는 마지막으로 받은 정보입니다.` : message);
    if (!state) showError('token', message);
  }
  updateControls();
}

function schedulePoll() {
  clearTimeout(pollTimer);
  if (connected) pollTimer = setTimeout(refresh, state?.phase === 'idle' ? 3000 : 900);
}

async function refresh() {
  if (pending) { schedulePoll(); return; }
  const epoch = ++requestEpoch;
  try {
    const next = await request('/api/state');
    if (epoch !== requestEpoch) return;
    render(next);
    connected = true;
    connectionMessage('');
    updateControls();
    schedulePoll();
  } catch (error) { if (epoch === requestEpoch) disconnected(error); }
}

async function connect() {
  if (pending) return;
  ++requestEpoch;
  clearTimeout(pollTimer);
  pending = true;
  updateControls();
  showError('token', '');
  byId('auth-submit').disabled = true;
  text('auth-submit', '연결 중…');
  byId('auth-form').setAttribute('aria-busy', 'true');
  try {
    const next = await request('/api/state');
    connected = true;
    saveToken();
    byId('auth-panel').hidden = true;
    byId('auth-panel').setAttribute('role', 'region');
    byId('app-shell').hidden = false;
    connectionMessage('');
    render(next);
    byId('main-content').focus();
    announce('DeltaWeave에 연결했습니다.');
  } catch (error) { disconnected(error); }
  finally {
    pending = false;
    byId('auth-submit').disabled = false;
    text('auth-submit', '연결하기');
    byId('auth-form').removeAttribute('aria-busy');
    updateControls();
    schedulePoll();
  }
}

function updateControls() {
  const actions = availableActions(state?.phase, connected && !pending, state?.activity[0]);
  byId('scan-button').disabled = !actions.scan;
  byId('sync-button').disabled = !actions.sync || !byId('confirm').checked;
  byId('receiver-start').disabled = !actions.start;
  byId('receiver-stop').disabled = !actions.stop;
  byId('copy-endpoint').disabled = !state?.endpoint_id;
  byId('reconnect').disabled = pending;
  for (const button of document.querySelectorAll('[data-retry-scan]')) button.disabled = !actions.scan;
  text('scan-button', state?.phase === 'scanning' ? '검사 중…' : '폴더 검사');
  text('sync-button', state?.phase === 'synchronizing' ? '동기화 중…' : '지금 동기화');
  text('receiver-start', state?.phase === 'starting_receiver' ? '수신 준비 중…' : '이 피어의 수신 대기');
  text('receiver-stop', state?.phase === 'stopping_receiver' ? actions.stop ? '수신 중지 다시 시도' : '안전하게 중지 중…' : '수신 대기 중지');
  byId('receiver-stop').hidden = !state?.receiver && state?.phase !== 'stopping_receiver';
  byId('peer-form').setAttribute('aria-busy', String(pending || state?.phase === 'synchronizing'));
}

function render(next) {
  state = next;
  const phase = phaseCopy(next.phase, next.activity[0]);
  text('root-path', next.root);
  text('endpoint-id', next.endpoint_id);
  text('version-label', `v${next.version}`);
  text('phase-label', phase.label);
  text('phase-description', phase.description);
  byId('phase-label').dataset.phase = next.phase;
  byId('receiver-info').hidden = !next.receiver;
  const addresses = next.receiver?.direct_addresses ?? [];
  const addressList = byId('receiver-addresses');
  const addressKey = JSON.stringify(addresses);
  if (addressList.dataset.addresses !== addressKey) {
    addressList.replaceChildren(...(addresses.length ? addresses : ['주소를 확인하고 있습니다.']).map(address => make('li', address)));
    addressList.dataset.addresses = addressKey;
  }
  const scanKey = JSON.stringify(next.scan);
  if (scanKey !== scanSignature) { renderScan(next.scan); scanSignature = scanKey; }
  const syncKey = JSON.stringify(next.sync);
  if (syncKey !== syncSignature) { renderSync(next.sync); syncSignature = syncKey; }
  const historyKey = JSON.stringify(next.activity);
  if (historyKey !== activitySignature) { renderActivity(next.activity); activitySignature = historyKey; }
  if (reportSignature !== scanKey + syncKey) {
    text('result-details', next.scan || next.sync ? JSON.stringify({ scan: next.scan, sync: next.sync }, null, 2) : '아직 완료된 결과가 없습니다.');
    reportSignature = scanKey + syncKey;
  }
  const latest = next.activity[0];
  const announcementKey = latest ? `${latest.id}:${latest.status}` : '';
  if (latest && announcementKey !== announcedActivity) {
    announce(latest.status === 'error' ? '작업을 완료하지 못했습니다. 실행 기록에서 원인과 복구 방법을 확인하세요.' : latest.status === 'running' ? phase.description : '작업이 완료되었습니다. 결과를 확인하세요.');
    announcedActivity = announcementKey;
  }
  updateControls();
}

function renderScan(scan) {
  byId('scan-summary').hidden = !scan;
  const records = scan?.records ?? [];
  byId('scan-empty').hidden = records.length > 0;
  text('scan-empty', scan ? '인덱스에 기록된 경로가 없습니다. 폴더에 파일을 추가한 뒤 다시 검사하세요.' : '폴더를 검사하면 실제 파일과 변경 내용을 확인할 수 있습니다.');
  text('scan-live-records', formatCount(scan?.report.live_records));
  text('scan-hashed', formatCount(scan?.report.files_hashed));
  text('scan-retries', formatCount(scan?.report.retries_queued));
  text('record-count', scan ? `기록 ${formatCount(scan.total_records)}개 중 ${formatCount(records.length)}개 표시${scan.total_records > records.length ? ' · 최대 200개' : ''}` : '검사 후 표시됩니다');
  const body = byId('file-table-body');
  body.closest('table').hidden = records.length === 0;
  const kind = { file: '파일', directory: '폴더', symlink: '심볼릭 링크', other: '기타' };
  body.replaceChildren(...records.map(record => {
    const row = document.createElement('tr');
    row.append(make('th', record.path, 'file-path'), make('td', kind[record.kind] ?? record.kind), make('td', record.kind === 'file' ? formatBytes(record.size) : '—'), make('td', recordStatus(record)));
    row.firstElementChild.scope = 'row';
    return row;
  }));
  const issues = byId('scan-issues');
  if (issues) {
    const items = (scan?.report.issues ?? []).map(issue => make('p', `${issue.path}: ${issue.message}`));
    const collisions = scan?.report.collisions ?? [];
    if (collisions.length) items.unshift(make('p', `다른 플랫폼에서 이름이 충돌하는 경로가 ${formatCount(collisions.length)}그룹 있습니다. 원본 보고서에서 경로를 확인하세요.`));
    issues.replaceChildren(...items);
    issues.hidden = items.length === 0;
  }
}

function renderSync(report) {
  const host = byId('sync-summary');
  if (!report) {
    text('sync-result-label', '아직 동기화하지 않았습니다');
    host.replaceChildren(make('p', '검증이 완료된 동기화의 전송량과 충돌 기록이 여기에 표시됩니다.'));
    return;
  }
  const verified = verifiedSync(report);
  text('sync-result-label', verified ? '양쪽 폴더 검증 완료' : '동기화 결과 확인 필요');
  const stats = make('dl', undefined, 'sync-metrics');
  for (const [label, value] of [
    ['보낸 데이터', formatBytes(report.pushed_bytes)],
    ['받은 데이터', formatBytes(report.pulled_bytes)],
    ['재사용한 청크 구간', formatCount(report.reused_extents)],
    ['충돌 기록', formatCount(report.conflicts?.length)],
  ]) {
    const group = document.createElement('div');
    group.append(make('dt', label), make('dd', value));
    stats.append(group);
  }
  host.replaceChildren(make('p', '최근 완료한 동기화 결과입니다. 이후 파일 변경은 다시 검사하거나 동기화해야 반영됩니다.'), stats);
  if (report.conflicts?.length) {
    const list = make('ul', undefined, 'conflict-list');
    for (const conflict of report.conflicts) {
      list.append(make('li', conflict.conflict_path ? `${conflict.path} · 보존된 사본: ${conflict.conflict_path}` : `${conflict.path} · 충돌 기록은 원본 보고서에서 확인하세요.`));
    }
    host.append(list);
  }
}

const operationNames = { scan: '폴더 검사', sync: '양방향 동기화', receiver_start: '수신 대기 시작', receiver_stop: '수신 대기 중지' };
const date = new Intl.DateTimeFormat('ko-KR', { month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
function renderActivity(items) {
  byId('activity-empty').hidden = items.length > 0;
  byId('activity-list').replaceChildren(...items.map(item => {
    const row = make('li', undefined, `activity-entry activity-${item.status}`);
    const summary = make('div', undefined, 'activity-heading');
    summary.append(make('strong', operationNames[item.kind] ?? item.kind), make('span', { running: '진행 중', success: '완료', error: '실패' }[item.status] ?? '상태 확인 필요', 'activity-status'));
    const timestamp = make('time', Number.isFinite(item.started_at_ms) ? date.format(item.started_at_ms) : '시간 정보 없음');
    if (Number.isFinite(item.started_at_ms)) timestamp.dateTime = new Date(item.started_at_ms).toISOString();
    row.append(summary, timestamp);
    if (item.error) {
      row.append(make('p', item.error, 'activity-error'));
      if (item.kind === 'scan') {
        const retry = make('button', '다시 검사', 'button button-secondary');
        retry.type = 'button';
        retry.dataset.retryScan = '';
        retry.disabled = !availableActions(state?.phase, connected && !pending).scan;
        retry.addEventListener('click', () => submit('/api/scan', {}));
        row.append(retry);
      } else {
        const link = make('a', '연결 설정 확인', 'text-link');
        link.href = '#connection';
        row.append(make('p', '입력값과 상대 기기의 수신 상태를 확인한 뒤 다시 실행하세요.'), link);
      }
    }
    return row;
  }));
}

async function submit(path, payload) {
  if (!connected || pending) return;
  ++requestEpoch;
  pending = true;
  clearTimeout(pollTimer);
  formMessage('');
  updateControls();
  try {
    render(await request(path, payload));
    announce(phaseCopy(state.phase).description);
  } catch (error) {
    if (error.status === 401 || error.status === 403 || !error.status) disconnected(error);
    else if (['peer_id', 'direct_address', 'confirm'].includes(error.field)) {
      showError(error.field, error.message);
      byId(error.field).focus();
    } else {
      formMessage(error.status === 409 ? '다른 작업이 진행 중입니다. 현재 작업이 끝난 뒤 다시 실행하세요.' : error.message);
      announce('요청을 처리하지 못했습니다. 안내를 확인하고 다시 실행하세요.');
    }
  } finally {
    pending = false;
    updateControls();
    schedulePoll();
  }
}

function connectPeer(kind) {
  const values = { peer_id: byId('peer_id').value.trim(), direct_address: byId('direct_address').value.trim(), confirm: byId('confirm').checked };
  const errors = validateConnection(values, state?.endpoint_id, kind);
  for (const field of ['peer_id', 'direct_address', 'confirm']) showError(field, errors[field]);
  formMessage('');
  const first = Object.keys(errors)[0];
  if (first) { byId(first).focus(); announce(errors[first]); return; }
  if (kind === 'receiver') void submit('/api/receiver/start', { peer_id: values.peer_id });
  else void submit('/api/sync', values);
}

byId('auth-form').addEventListener('submit', event => {
  event.preventDefault();
  if (pending) return;
  token = byId('token').value.trim();
  if (!token) { showError('token', '터미널에 표시된 접속 키를 입력하세요.'); byId('token').focus(); return; }
  void connect();
});
byId('peer-form').addEventListener('submit', event => { event.preventDefault(); connectPeer('sync'); });
byId('receiver-start').addEventListener('click', () => connectPeer('receiver'));
byId('receiver-stop').addEventListener('click', () => submit('/api/receiver/stop', {}));
byId('scan-button').addEventListener('click', () => submit('/api/scan', {}));
byId('reconnect').addEventListener('click', () => connect());
for (const id of ['peer_id', 'direct_address', 'confirm', 'token']) {
  byId(id).addEventListener('input', () => { showError(id, ''); updateControls(); });
}
byId('copy-endpoint').addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(state.endpoint_id);
    announce('이 기기의 피어 ID를 복사했습니다.');
    text('copy-endpoint', '복사됨');
    setTimeout(() => text('copy-endpoint', 'ID 복사'), 2000);
  } catch {
    const selection = getSelection();
    const range = document.createRange();
    range.selectNodeContents(byId('endpoint-id'));
    selection?.removeAllRanges();
    selection?.addRange(range);
    announce('자동 복사를 사용할 수 없습니다. 선택된 피어 ID를 직접 복사하세요.');
  }
});
window.addEventListener('pagehide', () => { ++requestEpoch; clearTimeout(pollTimer); });
window.addEventListener('pageshow', event => { if (event.persisted && connected) void refresh(); });

bootstrapToken();
updateControls();
if (token) void connect();
