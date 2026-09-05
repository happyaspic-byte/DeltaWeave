import test from 'node:test';
import assert from 'node:assert/strict';
import {
  validateConnection, formatBytes, formatCount, phaseCopy,
  verifiedSync, availableActions, recordStatus,
} from './model.js';

const peer = 'a'.repeat(64);
const self = 'b'.repeat(64);

test('sync requires a different valid peer, literal socket address and confirmation', () => {
  assert.deepEqual(validateConnection({ peer_id: peer, direct_address: '192.168.0.20:49152', confirm: true }, self), {});
  assert.deepEqual(validateConnection({ peer_id: peer, direct_address: '[::1]:49152', confirm: true }, self), {});
  const empty = validateConnection({ peer_id: '', direct_address: '', confirm: false }, self);
  assert.ok(empty.peer_id && empty.direct_address && empty.confirm);
  assert.ok(validateConnection({ peer_id: self, direct_address: '127.0.0.1:1', confirm: true }, self).peer_id);
  assert.ok(validateConnection({ peer_id: '<script>', direct_address: '127.0.0.1:1', confirm: true }, self).peer_id);
});

test('socket validation rejects ports and addresses the real API cannot use', () => {
  for (const address of ['localhost:80', '127.0.0.1:0', '127.0.0.1:65536', '999.1.1.1:12', '[::::]:12', '127.1:80', 'https://127.0.0.1:80', '127.0.0.1:abc']) {
    assert.ok(validateConnection({ peer_id: peer, direct_address: address, confirm: true }, self).direct_address, address);
  }
});

test('receiver authorization needs the peer ID but never demands a destination or sync consent', () => {
  assert.deepEqual(validateConnection({ peer_id: peer, direct_address: '', confirm: false }, self, 'receiver'), {});
  assert.ok(validateConnection({ peer_id: self }, self, 'receiver').peer_id);
});

test('unknown measurements stay distinct from verified zero', () => {
  assert.equal(formatBytes(null), '—');
  assert.equal(formatBytes(undefined), '—');
  assert.equal(formatBytes(-1), '—');
  assert.equal(formatBytes(0), '0 B');
  assert.equal(formatBytes(1024), '1 KiB');
  assert.equal(formatCount(null), '—');
  assert.equal(formatCount(0), '0');
});

test('only independently matching roots can be described as verified synchronization', () => {
  const report = { status: 'pass', desired_root: peer, verified_local_root: peer, verified_remote_root: peer };
  assert.equal(verifiedSync(report), true);
  assert.equal(verifiedSync({ ...report, verified_remote_root: self }), false);
  assert.equal(verifiedSync({ status: 'pass' }), false);
  assert.equal(verifiedSync(null), false);
});

test('receiver and local work remain exclusive and disconnected controls cannot mutate data', () => {
  assert.deepEqual(availableActions('idle', true), { scan: true, sync: true, start: true, stop: false });
  assert.deepEqual(availableActions('receiving', true), { scan: false, sync: false, start: false, stop: true });
  for (const phase of ['scanning', 'synchronizing', 'starting_receiver', 'stopping_receiver', 'unknown']) {
    assert.deepEqual(availableActions(phase, true), { scan: false, sync: false, start: false, stop: false });
  }
  assert.deepEqual(availableActions('idle', false), { scan: false, sync: false, start: false, stop: false });
  assert.equal(phaseCopy('unknown').label, '상태 확인 중');
});

test('record deletion and retry states are described with text instead of color only', () => {
  assert.equal(recordStatus({ tombstone: true }), '삭제 기록');
  assert.equal(recordStatus({ tombstone: false }), '인덱스에 기록됨');
});

test('a failed receiver drain permits only an explicit stop retry', () => {
  const stopped = { kind: 'receiver_stop', status: 'error', finished_at_ms: 123 };
  assert.deepEqual(availableActions('stopping_receiver', true, stopped), { scan: false, sync: false, start: false, stop: true });
  assert.equal(availableActions('stopping_receiver', true, { ...stopped, status: 'running' }).stop, false);
  assert.equal(availableActions('stopping_receiver', false, stopped).stop, false);
  assert.equal(phaseCopy('stopping_receiver', stopped).label, '중지 확인 필요');
});
