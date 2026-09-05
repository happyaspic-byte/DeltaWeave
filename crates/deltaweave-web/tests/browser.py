#!/usr/bin/env python3
"""Exercise the real web UI and two real local peers using agent-browser.

Build first: cargo build -p deltaweave-web -p deltaweave
Run: python3 crates/deltaweave-web/tests/browser.py --browser-cli /path/to/agent-browser
Optional: --browser-executable /path/to/chrome --no-sandbox --evidence /tmp/report
All roots, identities and peers are isolated test data. No production API mocks.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import tempfile
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-cli', default=shutil.which('agent-browser'))
    parser.add_argument('--browser-executable')
    parser.add_argument('--no-sandbox', action='store_true')
    parser.add_argument('--evidence', type=Path)
    args = parser.parse_args()
    if not args.browser_cli:
        parser.error('agent-browser is required; pass --browser-cli')
    repo = Path(__file__).resolve().parents[3]
    evidence = args.evidence or Path(tempfile.mkdtemp(prefix='deltaweave-web-browser-'))
    evidence.mkdir(parents=True, exist_ok=True)
    records, processes, sessions, logs, secrets = [], [], [], [], []
    session_prefix = f'deltaweave-e2e-{os.getpid()}'

    def redact(value):
        result = str(value)
        for secret in secrets:
            result = result.replace(secret, '<session-key>')
        return result

    def browser(session, *command):
        cli = [args.browser_cli, '--session', session, '--json']
        if args.browser_executable:
            cli += ['--executable-path', args.browser_executable]
        if args.no_sandbox:
            cli += ['--args', '--no-sandbox']
        output = subprocess.run(cli + list(command), capture_output=True, text=True, timeout=45)
        records.append({'session': session, 'command': [redact(c) for c in command],
                        'code': output.returncode, 'stdout': redact(output.stdout), 'stderr': redact(output.stderr)})
        if output.returncode:
            raise AssertionError(f'{command[0]} failed: {redact(output.stderr)} {redact(output.stdout)}')
        data = json.loads(output.stdout)
        assert data['success'], data
        return data.get('data', {})

    def evaluate(session, source):
        return browser(session, 'eval', source).get('result')

    def wait_ui(session, expression, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if evaluate(session, expression):
                return
            time.sleep(0.25)
        raise AssertionError(f'UI condition did not become true: {expression}')

    def api(node):
        req = urllib.request.Request(node['base'] + '/api/state', headers={'Authorization': f"Bearer {node['token']}"})
        with urllib.request.urlopen(req, timeout=5) as response:
            return json.load(response)

    def wait_operation(node, kind, status='success', after=0):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            state = api(node)
            matches = [a for a in state['activity'] if a['kind'] == kind and a['id'] > after and a['status'] != 'running']
            if matches:
                assert matches[0]['status'] == status, matches[0]
                return state
            time.sleep(0.15)
        raise AssertionError(f'{kind} did not finish')

    def capture(session, name, width, height=1000):
        browser(session, 'set', 'viewport', str(width), str(height))
        assert evaluate(session, 'document.documentElement.scrollWidth <= innerWidth'), f'Page overflow at {width}px'
        browser(session, 'screenshot', str(evidence / name), '--full')

    def launch(root, private, name):
        stderr = (evidence / f'{name}-server.log').open('w')
        logs.append(stderr)
        process = subprocess.Popen([str(repo / 'target/debug/deltaweave-web'), '--root', str(root), '--state', str(private),
                                    '--bind', '127.0.0.1:0', '--peer-bind', '127.0.0.1:0'], stdout=subprocess.PIPE, stderr=stderr, text=True)
        processes.append(process)
        # The startup line is emitted only after the real app and listener exist.
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=30), f'{name} startup timed out; see server log'
            line = process.stdout.readline()
        assert line, f'{name} did not start; see server log'
        startup = json.loads(line)
        base, fragment = startup['url'].split('/#token=')
        secrets.append(fragment)
        return {'base': base, 'token': fragment, 'id': startup['endpoint_id'], 'process': process}

    workspace_context = tempfile.TemporaryDirectory(prefix='deltaweave-real-browser-')
    try:
        # Keep the test roots alive until the peer processes have stopped.
        workspace = Path(workspace_context.name)
        a_root = workspace / '첫 번째 기기의 긴 폴더 English documents'
        b_root = workspace / 'NAS 동기화 폴더'
        a_root.mkdir()
        b_root.mkdir()
        a = launch(a_root, workspace / 'a-private', 'a')
        b = launch(b_root, workspace / 'b-private', 'b')
        a_session, b_session = session_prefix + '-a', session_prefix + '-b'
        sessions += [a_session, b_session]

        browser(a_session, 'open', a['base'] + '/#token=invalid')
        wait_ui(a_session, 'document.querySelector("#token").getAttribute("aria-invalid") === "true"')
        assert evaluate(a_session, 'location.hash === ""'), 'Token fragment was not removed'
        browser(a_session, 'fill', '#token', a['token'])
        browser(a_session, 'click', '#auth-submit')
        wait_ui(a_session, '!document.querySelector("#app-shell").hidden')
        browser(a_session, 'console', '--clear')
        browser(a_session, 'network', 'requests', '--clear')
        assert evaluate(a_session, 'document.querySelector("#sync-button").disabled'), 'Unconfirmed sync must be disabled'
        capture(a_session, 'web-initial-1440.png', 1440)

        # Delay one actual GET response to reproduce a stale polling race.
        # The response still comes from the real server; no state is fabricated.
        evaluate(a_session, '''(() => {
          const original = window.fetch.bind(window);
          let delayOne = true;
          window.fetch = async (...args) => {
            const response = await original(...args);
            if (delayOne && args[0] === '/api/state' && args[1]?.method === 'GET') {
              delayOne = false;
              return new Promise(resolve => { window.__releaseState = () => resolve(response); });
            }
            return response;
          };
          window.dispatchEvent(new PageTransitionEvent('pageshow', {persisted: true}));
          return true;
        })()''')
        wait_ui(a_session, 'typeof window.__releaseState === "function"')
        browser(a_session, 'click', '#scan-button')
        wait_operation(a, 'scan')
        wait_ui(a_session, 'document.querySelector("#scan-live-records").textContent === "0"')
        evaluate(a_session, 'window.__releaseState(); true')
        assert evaluate(a_session, 'document.querySelector("#scan-live-records").textContent === "0"'), 'An old poll replaced the newer completed scan'
        assert evaluate(a_session, '!document.querySelector("#scan-empty").hidden')

        long_name = '긴 한국어 파일 이름과 English filename ' + '문서 ' * 20 + '.txt'
        # Test data is labeled and private; none of these paths are user roots.
        (a_root / long_name).write_text('DeltaWeave browser regression fixture A\n', encoding='utf-8')
        (a_root / 'notes & 메모.txt').write_text('A second real file\n', encoding='utf-8')
        (b_root / 'NAS에서 받은 파일.txt').write_text('DeltaWeave browser regression fixture B\n', encoding='utf-8')
        last = api(a)['activity'][0]['id']
        browser(a_session, 'click', '#scan-button')
        wait_operation(a, 'scan', after=last)
        wait_ui(a_session, 'document.querySelector("#file-table-body").rows.length === 2')
        assert evaluate(a_session, 'document.querySelector("#file-table-body").textContent.includes("notes & 메모.txt")')

        # Real server rejection after authentication must retain the results,
        # move focus to recovery, and expose exactly one main landmark.
        evaluate(a_session, '''(() => {
          const original = window.fetch.bind(window);
          window.fetch = (...args) => {
            window.fetch = original;
            return original(args[0], {...args[1], headers: {...args[1].headers, Authorization: 'Bearer rejected-test-key'}});
          };
          window.dispatchEvent(new PageTransitionEvent('pageshow', {persisted: true}));
          return true;
        })()''')
        wait_ui(a_session, '!document.querySelector("#auth-panel").hidden && document.activeElement.id === "token"')
        assert evaluate(a_session, 'document.querySelector("#file-table-body").rows.length === 2 && document.querySelector("#scan-button").disabled')
        assert evaluate(a_session, '[...document.querySelectorAll("main,[role=main]")].filter(e => e.checkVisibility()).length === 1')
        browser(a_session, 'fill', '#token', a['token'])
        browser(a_session, 'click', '#auth-submit')
        wait_ui(a_session, 'document.querySelector("#auth-panel").hidden && !document.querySelector("#scan-button").disabled')
        # The rejected request above is intentional; remaining requests must succeed.
        browser(a_session, 'console', '--clear')
        browser(a_session, 'network', 'requests', '--clear')

        browser(a_session, 'check', '#confirm')
        assert evaluate(a_session, '!document.querySelector("#sync-button").disabled'), 'Confirmation did not enable submission'
        browser(a_session, 'click', '#sync-button')
        assert evaluate(a_session, 'document.activeElement.id === "peer_id" && !document.querySelector("#peer_id-error").hidden')
        browser(a_session, 'fill', '#peer_id', b['id'])
        browser(a_session, 'fill', '#direct_address', '127.0.0.1:65536')
        browser(a_session, 'click', '#sync-button')
        assert evaluate(a_session, 'document.activeElement.id === "direct_address" && !document.querySelector("#direct_address-error").hidden')

        browser(b_session, 'open', b['base'] + '/#token=' + b['token'])
        wait_ui(b_session, '!document.querySelector("#app-shell").hidden')
        third = json.loads(subprocess.check_output([str(repo / 'target/debug/deltaweave'), 'init', '--identity', str(workspace / 'third.key')], text=True))
        browser(b_session, 'fill', '#peer_id', third['endpoint_id'])
        browser(b_session, 'click', '#receiver-start')
        b_state = wait_operation(b, 'receiver_start')
        wait_ui(b_session, '!document.querySelector("#receiver-stop").disabled')
        assert evaluate(b_session, 'document.querySelector("#scan-button").disabled && document.querySelector("#sync-button").disabled')
        denied_address = b_state['receiver']['direct_addresses'][0]
        browser(a_session, 'fill', '#direct_address', denied_address)
        browser(a_session, 'click', '#sync-button')
        failed = wait_operation(a, 'sync', status='error')
        wait_ui(a_session, 'document.querySelector("#activity-list").textContent.includes("실패") && !document.querySelector("#sync-button").disabled')
        assert evaluate(a_session, 'document.querySelector("#peer_id").value') == b['id']
        assert evaluate(a_session, 'document.querySelector("#direct_address").value') == denied_address
        assert failed['sync'] is None
        capture(a_session, 'web-peer-error-768.png', 768)

        browser(b_session, 'click', '#receiver-stop')
        wait_operation(b, 'receiver_stop')
        wait_ui(b_session, '!document.querySelector("#receiver-start").disabled')
        browser(b_session, 'fill', '#peer_id', a['id'])
        last_b = api(b)['activity'][0]['id']
        browser(b_session, 'click', '#receiver-start')
        b_state = wait_operation(b, 'receiver_start', after=last_b)
        browser(a_session, 'fill', '#direct_address', b_state['receiver']['direct_addresses'][0])
        last_a = api(a)['activity'][0]['id']
        browser(a_session, 'click', '#sync-button')
        synced = wait_operation(a, 'sync', after=last_a)
        wait_ui(a_session, 'document.querySelector("#sync-result-label").textContent === "양쪽 폴더 검증 완료"')
        report = synced['sync']
        assert report['verified_local_root'] == report['verified_remote_root'] == report['desired_root']
        assert report['pushed_bytes'] > 0 and report['pulled_bytes'] > 0
        for path in a_root.iterdir():
            assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256((b_root / path.name).read_bytes()).digest()
        assert sorted(p.name for p in a_root.iterdir()) == sorted(p.name for p in b_root.iterdir())

        last_a = api(a)['activity'][0]['id']
        browser(a_session, 'click', '#scan-button')
        wait_operation(a, 'scan', after=last_a)
        wait_ui(a_session, 'document.querySelector("#file-table-body").rows.length === 3')
        for width in (360, 768, 1440):
            capture(a_session, f'web-verified-{width}-light.png', width, 900 if width == 360 else 1000)
        browser(a_session, 'set', 'media', 'light', 'reduced-motion')
        assert evaluate(a_session, 'matchMedia("(prefers-reduced-motion: reduce)").matches')
        browser(a_session, 'focus', '#scan-button')
        browser(a_session, 'press', 'Tab')
        focus = evaluate(a_session, '({id:document.activeElement.id,outline:getComputedStyle(document.activeElement).outlineStyle,width:getComputedStyle(document.activeElement).outlineWidth})')
        assert focus['id'] == 'copy-endpoint' and focus['outline'] != 'none' and focus['width'] != '0px', focus
        browser(a_session, 'click', 'a[href="#connection"]')
        assert evaluate(a_session, 'location.hash === "#connection"')
        browser(a_session, 'click', '.result-disclosure summary')
        assert evaluate(a_session, 'document.querySelector(".result-disclosure").open')
        browser(a_session, 'click', '.result-disclosure summary')
        runtime_errors = browser(a_session, 'errors')
        records.append({'runtime_errors': runtime_errors})
        assert runtime_errors.get('errors') == [], runtime_errors
        console = browser(a_session, 'console')
        assert not [m for m in console.get('messages', []) if m.get('type') == 'error'], console
        network = browser(a_session, 'network', 'requests')
        unexpected = [r for r in network.get('requests', []) if r.get('failure') or r.get('status', 200) >= 400]
        assert not unexpected, redact(unexpected)

        browser(b_session, 'click', '#receiver-stop')
        wait_operation(b, 'receiver_stop', after=last_b)
        a['process'].terminate()
        a['process'].wait(timeout=30)
        wait_ui(a_session, '!document.querySelector("#connection-banner").hidden')
        assert evaluate(a_session, 'document.querySelector("#scan-button").disabled && document.querySelector("#peer_id").value.length === 64')
        browser(a_session, 'click', '#reconnect')
        wait_ui(a_session, '!document.querySelector("#connection-banner").hidden && document.querySelector("#scan-button").disabled')
        capture(a_session, 'web-disconnected-768.png', 768)
        records.append({'result': 'passed', 'actual_files_per_peer': 3,
                        'pushed_bytes': report['pushed_bytes'], 'pulled_bytes': report['pulled_bytes'],
                        'verified_root': report['desired_root'], 'ui_viewports': [360, 768, 1440]})
        print(f'Browser regression passed; real two-peer files verified. Evidence: {evidence}')
    finally:
        for session in sessions:
            try:
                browser(session, 'close')
            except Exception as error:
                records.append({'cleanup_error': redact(error)})
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in logs:
            log.close()
        workspace_context.cleanup()
        (evidence / 'browser-results.json').write_text(json.dumps(records, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()
