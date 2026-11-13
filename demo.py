import json
from pathlib import Path

import collector
from service import notice_key


def _load_rounds():
    scans = json.loads((Path(__file__).parent / 'fixtures/scans.json').read_text(encoding='utf-8'))
    return scans['rounds']


def _replay_all(state, rounds, mode):
    return [collector.replay_round(state, round_dict, mode=mode) for round_dict in rounds]


def run():
    rounds = _load_rounds()
    first = rounds[0]
    keys = [notice_key(row['host'], row['id']) for page in first['pages'] for row in page]
    assert len(set(keys)) == 2

    # 全量模式重放三轮：同秒并列、跨页漂移、更正、完整扫描后的淘汰
    full = collector.new_state()
    _replay_all(full, rounds, 'full')
    full_snapshot = json.dumps(full, sort_keys=True)

    # 重放相同批次必须无副作用
    replay_again = _replay_all(full, rounds, 'full')
    idempotent = (all(report['skipped'] for report in replay_again)
                  and json.dumps(full, sort_keys=True) == full_snapshot)

    # 在 drift 轮第 2 页前注入故障，随后从断点续跑，结果须与无故障运行一致
    crashed = collector.new_state()
    collector.replay_round(crashed, rounds[0], mode='full')
    try:
        collector.replay_round(crashed, rounds[1], mode='full', fail_at=('drift', 1))
        raise AssertionError('expected SimulatedCrash')
    except collector.SimulatedCrash:
        pass
    _replay_all(crashed, rounds, 'full')
    resume_ok = json.dumps(crashed, sort_keys=True) == full_snapshot

    # 增量模式对照：游标覆盖处截断，永不淘汰索引
    incremental = collector.new_state()
    _replay_all(incremental, rounds, 'incremental')

    return {
        'first_scan_keys': keys,
        'available_rounds': [round_dict['scan'] for round_dict in rounds],
        'workflow': {
            'mode': 'full',
            'cursor': collector.cursor_snapshot(full),
            'retired': [key for key, entry in sorted(full['index'].items())
                        if entry['status'] == 'retired'],
            'corrections': full['cursor']['corrections'],
            'outbox': full['outbox'],
            'audit_entries': len(full['audit']),
            'idempotent_replay': idempotent,
            'resume_matches_uninterrupted': resume_ok,
            'incremental_retired': [key for key, entry in sorted(incremental['index'].items())
                                    if entry['status'] == 'retired'],
            'lineage_of_a.example:7': collector.lineage(full, 'a.example:7'),
        },
    }


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
