const number = new Intl.NumberFormat('ko-KR', { maximumFractionDigits: 1 });

export function formatCount(value) {
  return Number.isFinite(value) && value >= 0 ? number.format(value) : '—';
}

export function formatBytes(value) {
  if (!Number.isFinite(value) || value < 0) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  const exponent = value === 0 ? 0 : Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${number.format(value / 1024 ** exponent)} ${units[exponent]}`;
}

function validSocket(address) {
  const match = /^(\[[0-9a-f:]+\]|(?:\d{1,3}\.){3}\d{1,3}):(\d{1,5})$/i.exec(address);
  if (!match || Number(match[2]) < 1 || Number(match[2]) > 65535) return false;
  if (!match[1].startsWith('[')) return match[1].split('.').every(part => Number(part) <= 255);
  try {
    return new URL(`http://${address}`).hostname.startsWith('[');
  } catch {
    return false;
  }
}

export function validateConnection(values, selfId, kind = 'sync') {
  const errors = {};
  const peer = String(values.peer_id ?? '').trim();
  if (!/^[0-9a-f]{64}$/i.test(peer)) {
    errors.peer_id = '상대 기기에 표시된 64자리 피어 ID를 입력하세요.';
  } else if (peer.toLowerCase() === String(selfId ?? '').toLowerCase()) {
    errors.peer_id = '이 기기의 ID입니다. 연결할 상대 기기의 ID를 입력하세요.';
  }
  if (kind === 'sync') {
    if (!validSocket(String(values.direct_address ?? '').trim())) {
      errors.direct_address = 'IP 주소와 포트를 확인하세요. 예: 192.168.0.20:49152';
    }
    if (values.confirm !== true) {
      errors.confirm = '양쪽 폴더에 변경 사항이 적용되는 것을 확인해 주세요.';
    }
  }
  return errors;
}

const phases = {
  idle: { label: '준비됨', description: '폴더를 검사하거나 상대 기기와 동기화할 수 있습니다.' },
  scanning: { label: '폴더 검사 중', description: '실제 파일과 인덱스를 비교하고 있습니다. 완료되면 결과가 표시됩니다.' },
  synchronizing: { label: '동기화 중', description: '양쪽 폴더의 변경 내용을 적용하고 결과를 검증하고 있습니다.' },
  starting_receiver: { label: '수신 준비 중', description: '허용한 피어를 위한 암호화 연결을 준비하고 있습니다.' },
  receiving: { label: '수신 대기 중', description: '허용한 피어의 연결을 받고 있습니다. 폴더 검사나 동기화를 실행하려면 수신 대기를 중지하세요.' },
  stopping_receiver: { label: '수신 대기 중지 중', description: '연결과 진행 중인 파일 작업을 안전하게 정리하고 있습니다.' },
};

function stopNeedsRetry(phase, latest) {
  return phase === 'stopping_receiver' && latest?.kind === 'receiver_stop'
    && latest.status === 'error' && Number.isFinite(latest.finished_at_ms);
}

export function phaseCopy(phase, latest) {
  if (stopNeedsRetry(phase, latest)) return { label: '중지 확인 필요', description: '파일 작업이 안전하게 정리됐는지 확인하지 못했습니다. 작업 기록을 확인하고 수신 대기 중지를 다시 시도하세요.' };
  return phases[phase] ?? { label: '상태 확인 중', description: '서버에서 현재 상태를 확인하고 있습니다.' };
}

export function availableActions(phase, connected, latest) {
  return {
    scan: connected && phase === 'idle',
    sync: connected && phase === 'idle',
    start: connected && phase === 'idle',
    stop: connected && (phase === 'receiving' || stopNeedsRetry(phase, latest)),
  };
}

export function verifiedSync(report) {
  return report?.status === 'pass'
    && typeof report.desired_root === 'string'
    && report.desired_root.length === 64
    && report.desired_root === report.verified_local_root
    && report.desired_root === report.verified_remote_root;
}

export function recordStatus(record) {
  return record.tombstone ? '삭제 기록' : '인덱스에 기록됨';
}
