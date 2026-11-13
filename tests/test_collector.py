import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import collector

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'fixtures' / 'scans.json'
FIRST_TS = '2026-11-11T23:59:59+08:00'
SECOND_TS = '2026-11-12T00:00:00+08:00'


def load_rounds():
    return json.loads(FIXTURES.read_text(encoding='utf-8'))['rounds']


def replay_all(state, rounds, mode='full'):
    return [collector.replay_round(state, round_dict, mode=mode) for round_dict in rounds]


class CursorTests(unittest.TestCase):
    def setUp(self):
        self.rounds = load_rounds()

    def test_identity_scoped_to_host(self):
        """不同平台的相同公告号是两条记录。"""
        state = collector.new_state()
        collector.replay_round(state, self.rounds[0], mode='full')
        self.assertEqual(sorted(state['index']), ['a.example:7', 'b.example:7'])
        kinds = [(event['kind'], event['key']) for event in state['outbox']]
        self.assertEqual(kinds, [('new', 'a.example:7'), ('new', 'b.example:7')])

    def test_cursor_covers_tied_timestamps(self):
        """游标记录同秒全部公告键；并列时间的新公告不被时间戳吞掉。"""
        state = collector.new_state()
        collector.replay_round(state, self.rounds[0], mode='full')
        cursor = collector.cursor_snapshot(state)
        self.assertEqual(cursor['high_water'],
                         {'time': FIRST_TS, 'ties': ['a.example:7', 'b.example:7']})
        # 模拟跨午夜重启后补到一条同秒公告：仅靠最后时间戳会漏掉它
        late = {'scan': 'late', 'complete': False, 'pages': [[
            {'host': 'c.example', 'id': '9', 'time': FIRST_TS, 'amount': '5.00'}]]}
        report = collector.replay_round(state, late, mode='incremental')
        self.assertEqual(report['pages'][0]['new'], ['c.example:9'])
        self.assertEqual(
            collector.cursor_snapshot(state)['high_water']['ties'],
            ['a.example:7', 'b.example:7', 'c.example:9'])

    def test_page_drift_does_not_duplicate(self):
        """公告跨页移动后按身份去重，不重复发下游。"""
        state = collector.new_state()
        replay_all(state, self.rounds[:2])
        moved = [event for event in state['outbox']
                 if event['key'] == 'a.example:7' and event['kind'] == 'new']
        self.assertEqual(len(moved), 1)
        self.assertEqual(state['cursor']['corrections'], {'a.example:8': 'a.example:7'})

    def test_correction_links_recorded_in_cursor(self):
        state = collector.new_state()
        reports = replay_all(state, self.rounds[:2])
        drift_page = reports[1]['pages'][0]
        self.assertEqual(drift_page['corrections'],
                         [{'key': 'a.example:8', 'corrects': 'a.example:7'}])
        self.assertEqual(drift_page['cursor']['corrections'],
                         {'a.example:8': 'a.example:7'})


class RetirementTests(unittest.TestCase):
    def setUp(self):
        self.rounds = load_rounds()

    def test_incomplete_scan_retires_nothing(self):
        """未标记完整的扫描（drift 轮）不淘汰任何索引。"""
        state = collector.new_state()
        replay_all(state, self.rounds[:2])
        self.assertTrue(all(entry['status'] == 'active'
                            for entry in state['index'].values()))

    def test_retirement_after_complete_full_scan_keeps_audit(self):
        """完整全量扫描结束后才淘汰消失记录，审计历史与原文摘要保留。"""
        state = collector.new_state()
        replay_all(state, self.rounds)
        retired = [key for key, entry in sorted(state['index'].items())
                   if entry['status'] == 'retired']
        self.assertEqual(retired, ['a.example:7', 'b.example:7'])
        self.assertEqual(state['index']['a.example:8']['status'], 'active')
        # 三轮共 5 条观察全部保留，被淘汰公告的原金额仍可查
        self.assertEqual(len(state['audit']), 5)
        original = [entry for entry in state['audit'] if entry['key'] == 'a.example:7']
        self.assertEqual(len(original), 2)
        self.assertTrue(all(entry['payload']['amount'] == '100.00' for entry in original))
        self.assertEqual(state['index']['a.example:7']['payload']['amount'], '100.00')

    def test_incremental_mode_never_retires(self):
        """增量模式即使遇到完整轮次也不淘汰，且在游标覆盖处截断。"""
        state = collector.new_state()
        reports = replay_all(state, self.rounds, mode='incremental')
        self.assertTrue(all(entry['status'] == 'active'
                            for entry in state['index'].values()))
        self.assertEqual([event for event in state['outbox'] if event['kind'] == 'retire'],
                         [])
        drift = reports[1]
        self.assertEqual(drift['pages'][1]['reason'], 'incremental-cutoff')
        final = reports[2]
        self.assertEqual(final['pages'][0]['reason'], 'incremental-cutoff')

    def test_correction_lineage_queryable_after_retirement(self):
        """更正前后关联在淘汰后仍可查询，原文摘要保留。"""
        state = collector.new_state()
        replay_all(state, self.rounds)
        original = collector.lineage(state, 'a.example:7')
        self.assertEqual(original['corrected_by'], ['a.example:8'])
        self.assertEqual(original['chain_forward'], ['a.example:8'])
        self.assertEqual(original['current']['status'], 'retired')
        self.assertEqual(original['current']['payload']['amount'], '100.00')
        correction = collector.lineage(state, 'a.example:8')
        self.assertEqual(correction['corrects'], 'a.example:7')
        self.assertEqual(correction['chain_back'], ['a.example:7'])
        self.assertEqual(correction['current']['status'], 'active')


class ReplaySafetyTests(unittest.TestCase):
    def setUp(self):
        self.rounds = load_rounds()

    def test_replay_same_batch_has_no_side_effects(self):
        state = collector.new_state()
        replay_all(state, self.rounds)
        snapshot = json.dumps(state, sort_keys=True)
        again = replay_all(state, self.rounds)
        self.assertTrue(all(report['skipped'] for report in again))
        self.assertEqual(json.dumps(state, sort_keys=True), snapshot)
        ids = [event['id'] for event in state['outbox']]
        self.assertEqual(len(ids), len(set(ids)))

    def test_resume_from_any_page_boundary(self):
        """在任意一页之前失败，续跑结果与无故障运行完全一致。"""
        reference = collector.new_state()
        replay_all(reference, self.rounds)
        expected = json.dumps(reference, sort_keys=True)
        for round_dict in self.rounds:
            for page in range(len(round_dict['pages'])):
                with self.subTest(scan=round_dict['scan'], page=page):
                    crashed = collector.new_state()
                    with self.assertRaises(collector.SimulatedCrash):
                        for item in self.rounds:
                            collector.replay_round(
                                crashed, item, mode='full',
                                fail_at=(round_dict['scan'], page))
                    replay_all(crashed, self.rounds)
                    self.assertEqual(json.dumps(crashed, sort_keys=True), expected)

    def test_resume_tolerates_drifted_page_content(self):
        """重启期间页面内容漂移：未提交的页按新内容应用，旧版本留在审计里。"""
        crashed = collector.new_state()
        collector.replay_round(crashed, self.rounds[0], mode='full')
        with self.assertRaises(collector.SimulatedCrash):
            collector.replay_round(crashed, self.rounds[1], mode='full',
                                   fail_at=('drift', 1))
        drifted = json.loads(json.dumps(self.rounds[1]))
        drifted['pages'][1][0]['amount'] = '105.00'
        collector.replay_round(crashed, drifted, mode='full')
        entry = crashed['index']['a.example:7']
        self.assertEqual(entry['payload']['amount'], '105.00')
        self.assertIn(f"updated:a.example:7:{entry['digest']}",
                      [event['id'] for event in crashed['outbox']])
        digests = {item['digest'] for item in crashed['audit']
                   if item['key'] == 'a.example:7'}
        self.assertEqual(len(digests), 2)

    def test_page_gap_is_rejected(self):
        state = collector.new_state()
        with self.assertRaises(ValueError):
            collector.apply_page(state, 'first', 1, self.rounds[0]['pages'][1],
                                 mode='full')


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state = str(Path(self.tmp.name) / 'state.json')

    def run_cli(self, *args):
        return subprocess.run([sys.executable, 'collector.py', *args], cwd=ROOT,
                              text=True, capture_output=True, timeout=30)

    def test_replay_reports_cursor_and_diff(self):
        result = self.run_cli('replay', '--state', self.state, '--mode', 'full')
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report['cursor']['high_water'],
                         {'time': SECOND_TS, 'ties': ['a.example:8']})
        self.assertEqual(report['cursor']['corrections'],
                         {'a.example:8': 'a.example:7'})
        self.assertEqual(report['rounds'][2]['close']['retired'],
                         ['a.example:7', 'b.example:7'])
        self.assertEqual(len(report['outbox']), 6)
        # 重放相同批次无副作用
        again = self.run_cli('replay', '--state', self.state, '--mode', 'full')
        self.assertEqual(again.returncode, 0, again.stderr)
        repeated = json.loads(again.stdout)
        self.assertTrue(all(item['skipped'] for item in repeated['rounds']))
        self.assertEqual(len(repeated['outbox']), 6)

    def test_incremental_mode_reports_no_retirement(self):
        result = self.run_cli('replay', '--state', self.state, '--mode', 'incremental')
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report['rounds'][2]['close']['retired'], [])
        self.assertEqual(report['rounds'][1]['pages'][1]['reason'],
                         'incremental-cutoff')

    def test_crash_exit_code_and_resume_matches_clean_run(self):
        crashed = self.run_cli('replay', '--state', self.state, '--mode', 'full',
                               '--fail-at', 'drift:1')
        self.assertEqual(crashed.returncode, 3)
        payload = json.loads(crashed.stdout)
        self.assertEqual(payload['error'],
                         {'kind': 'simulated-crash', 'scan': 'drift', 'page': 1})
        self.assertEqual(payload['cursor']['scan'],
                         {'id': 'drift', 'next_page': 1, 'complete': False})
        resumed = self.run_cli('replay', '--state', self.state, '--mode', 'full')
        self.assertEqual(resumed.returncode, 0, resumed.stderr)
        clean_state = str(Path(self.tmp.name) / 'clean.json')
        clean = self.run_cli('replay', '--state', clean_state, '--mode', 'full')
        self.assertEqual(json.loads(resumed.stdout)['cursor'],
                         json.loads(clean.stdout)['cursor'])
        self.assertEqual(json.loads(resumed.stdout)['outbox'],
                         json.loads(clean.stdout)['outbox'])

    def test_status_and_lineage_commands(self):
        self.assertEqual(self.run_cli('replay', '--state', self.state,
                                      '--mode', 'full').returncode, 0)
        status = json.loads(self.run_cli('status', '--state', self.state).stdout)
        self.assertEqual(status['index']['a.example:7']['status'], 'retired')
        self.assertEqual(status['index']['a.example:7']['retired_scan'], 'final')
        self.assertEqual(status['index']['a.example:8']['status'], 'active')
        self.assertEqual(status['audit_entries'], 5)
        lineage = json.loads(self.run_cli('lineage', '--state', self.state,
                                          '--key', 'a.example:7').stdout)
        self.assertEqual(lineage['corrected_by'], ['a.example:8'])
        self.assertEqual(lineage['current']['payload']['amount'], '100.00')


if __name__ == '__main__':
    unittest.main()
