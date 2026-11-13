"""公告采集工作流：增量/全量扫描、复合游标、更正关联与幂等重放。

公告身份由 ``service.notice_key`` 的（来源域, 来源编号）复合键确定，
不同平台的相同编号互不冲突。

游标不是单一时间戳，而是四元信息：

* ``high_water``：最高发布时间 + 该时刻已见的全部公告键（覆盖并列发布时间，
  跨午夜重启后同秒公告既不漏抓也不重复）；
* ``scan``：进行中轮次的标识、下一页序号与已提交页哈希（覆盖页面分页漂移，
  支持从任意页失败后继续）；
* ``corrections``：更正公告 -> 原公告的关联（覆盖公告更正关系，可双向查询，
  原文摘要随审计日志保留）；
* ``mode``：最近一次使用的扫描模式（``full`` / ``incremental``）。

副作用规则：

* 下游发送（outbox）按事件 id 去重，重放相同批次不产生任何变化；
* 审计日志（audit）只追加，永不清除；
* 只有标记为完整（complete）的全量（full）扫描结束后，才把当前索引中
  消失的记录标记为 ``retired``；增量扫描永不淘汰。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path

from service import notice_key

SCHEMA_VERSION = 1
MODES = ('full', 'incremental')

Rec = namedtuple('Rec', ['key', 'time', 'digest', 'corrects_key', 'payload'])


class SimulatedCrash(Exception):
    """在指定扫描页之前注入的故障，用于验证断点续跑。"""

    def __init__(self, scan, page):
        super().__init__(f'simulated crash before {scan} page {page}')
        self.scan = scan
        self.page = page


# ---------------------------------------------------------------- 记录规范化

def digest_row(row):
    """原始行的内容摘要（canonical JSON 的 SHA-256）。"""
    blob = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


def normalize_row(row):
    host = row.get('host')
    notice_id = row.get('id')
    key = notice_key(
        str(host) if host is not None else None,
        str(notice_id) if notice_id is not None else None,
    )
    corrects = row.get('corrects')
    corrects_key = None
    if corrects not in (None, ''):
        # 更正目标与被更正公告同属一个来源域
        corrects_key = notice_key(str(host), str(corrects))
    return Rec(
        key=key,
        time=row.get('time'),
        digest=digest_row(row),
        corrects_key=corrects_key,
        payload=dict(row),
    )


def _time_key(value):
    """把发布时间规范为可比较元组；无法解析的值排在已解析值之后。"""
    if value is None:
        return (2, '')
    try:
        moment = datetime.fromisoformat(str(value))
    except ValueError:
        return (1, str(value))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return (0, moment.astimezone(timezone.utc).isoformat())


def _page_hash(scan_id, page_index, rows):
    blob = json.dumps({'scan': scan_id, 'page': page_index, 'rows': rows},
                      sort_keys=True, ensure_ascii=False, separators=(',', ':'))
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


# ---------------------------------------------------------------- 状态存取

def new_state():
    return {
        'version': SCHEMA_VERSION,
        'cursor': {'mode': None, 'high_water': None, 'scan': None, 'corrections': {}},
        'index': {},
        'audit': [],
        'outbox': [],
        'scans': {},
    }


def load_state(path):
    path = Path(path)
    if not path.exists():
        return new_state()
    return json.loads(path.read_text(encoding='utf-8'))


def save_state(path, state):
    """原子落盘：先写临时文件再替换，崩溃不会留下半个状态。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp')
    tmp.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True, indent=2),
                   encoding='utf-8')
    os.replace(tmp, path)


def _new_scan():
    return {'complete': None, 'closed': False, 'cutoff': False, 'pages': {}, 'seen': []}


# ---------------------------------------------------------------- 游标

def cursor_snapshot(state):
    cursor = state['cursor']
    high_water = cursor['high_water']
    return {
        'mode': cursor['mode'],
        'high_water': {'time': high_water['time'], 'ties': list(high_water['ties'])}
        if high_water else None,
        'scan': dict(cursor['scan']) if cursor['scan'] else None,
        'corrections': dict(sorted(cursor['corrections'].items())),
    }


def _is_beyond(cursor, rec):
    """该记录是否超出当前游标（并列时间按公告键逐一判定）。"""
    high_water = cursor['high_water']
    if high_water is None:
        return True
    marker = _time_key(rec.time)
    boundary = _time_key(high_water['time'])
    if marker > boundary:
        return True
    if marker == boundary:
        return rec.key not in high_water['ties']
    return False


def _advance(cursor, rec):
    if rec.time is None:
        return
    high_water = cursor['high_water']
    marker = _time_key(rec.time)
    if high_water is None or marker > _time_key(high_water['time']):
        cursor['high_water'] = {'time': rec.time, 'ties': [rec.key]}
    elif marker == _time_key(high_water['time']):
        if rec.key not in high_water['ties']:
            high_water['ties'].append(rec.key)
            high_water['ties'].sort()


# ---------------------------------------------------------------- 副作用

def _emit(state, event):
    """下游发送按事件 id 去重：补跑不会重复推送历史记录。"""
    if all(existing['id'] != event['id'] for existing in state['outbox']):
        state['outbox'].append(event)


def _audit(state, scan_id, page_index, rec):
    seen = any(
        entry['scan'] == scan_id and entry['page'] == page_index
        and entry['key'] == rec.key and entry['digest'] == rec.digest
        for entry in state['audit']
    )
    if not seen:
        state['audit'].append({
            'seq': len(state['audit']),
            'scan': scan_id,
            'page': page_index,
            'key': rec.key,
            'digest': rec.digest,
            'payload': rec.payload,
        })


# ---------------------------------------------------------------- 页应用

def apply_page(state, scan_id, page_index, rows, *, mode):
    """应用一页分页响应，返回该页差异。已提交的相同页直接跳过（幂等）。"""
    scan = state['scans'].setdefault(scan_id, _new_scan())
    if scan['closed']:
        return {'scan': scan_id, 'page': page_index, 'skipped': True,
                'reason': 'scan-closed', 'rows': len(rows),
                'cursor': cursor_snapshot(state)}
    page_hash = _page_hash(scan_id, page_index, rows)
    committed = scan['pages'].get(str(page_index))
    if committed == page_hash:
        return {'scan': scan_id, 'page': page_index, 'skipped': True,
                'reason': 'page-committed', 'rows': len(rows),
                'cursor': cursor_snapshot(state)}
    if committed is not None:
        raise ValueError(
            f'page {page_index} of scan {scan_id!r} already committed with different content')
    expected = len(scan['pages'])
    if page_index != expected:
        raise ValueError(
            f'page gap in scan {scan_id!r}: expected page {expected}, got {page_index}')

    recs = [normalize_row(row) for row in rows]
    diff = {
        'scan': scan_id, 'page': page_index, 'skipped': False, 'rows': len(rows),
        'beyond_cursor': sum(1 for rec in recs if _is_beyond(state['cursor'], rec)),
        'new': [], 'updated': [], 'reinstated': [], 'corrections': [],
    }
    for rec in recs:
        _audit(state, scan_id, page_index, rec)
        entry = state['index'].get(rec.key)
        if entry is None:
            state['index'][rec.key] = {
                'payload': rec.payload, 'digest': rec.digest,
                'first_scan': scan_id, 'last_scan': scan_id, 'status': 'active',
            }
            _emit(state, {'id': f'new:{rec.key}:{rec.digest}', 'kind': 'new',
                          'key': rec.key, 'scan': scan_id, 'digest': rec.digest})
            diff['new'].append(rec.key)
        elif entry['status'] == 'retired':
            entry.update(payload=rec.payload, digest=rec.digest,
                         last_scan=scan_id, status='active')
            entry.pop('retired_scan', None)
            _emit(state, {'id': f'reinstated:{rec.key}:{scan_id}', 'kind': 'reinstated',
                          'key': rec.key, 'scan': scan_id, 'digest': rec.digest})
            diff['reinstated'].append(rec.key)
        elif entry['digest'] != rec.digest:
            entry.update(payload=rec.payload, digest=rec.digest, last_scan=scan_id)
            _emit(state, {'id': f'updated:{rec.key}:{rec.digest}', 'kind': 'updated',
                          'key': rec.key, 'scan': scan_id, 'digest': rec.digest})
            diff['updated'].append(rec.key)
        else:
            entry['last_scan'] = scan_id
        if rec.corrects_key is not None:
            edges = state['cursor']['corrections']
            if edges.get(rec.key) != rec.corrects_key:
                edges[rec.key] = rec.corrects_key
                _emit(state, {'id': f'correction:{rec.key}:{rec.corrects_key}',
                              'kind': 'correction', 'key': rec.key,
                              'scan': scan_id, 'corrects': rec.corrects_key})
                diff['corrections'].append({'key': rec.key, 'corrects': rec.corrects_key})
        if rec.key not in scan['seen']:
            scan['seen'].append(rec.key)
        _advance(state['cursor'], rec)
    scan['pages'][str(page_index)] = page_hash
    state['cursor']['scan'] = {'id': scan_id, 'next_page': page_index + 1,
                               'complete': scan['complete']}
    state['cursor']['mode'] = mode
    diff['cursor'] = cursor_snapshot(state)
    return diff


def _page_is_stale(state, rows):
    """整页都已被游标和索引覆盖（增量模式的截断条件）。"""
    cursor = state['cursor']
    for row in rows:
        rec = normalize_row(row)
        if _is_beyond(cursor, rec):
            return False
        entry = state['index'].get(rec.key)
        if entry is None or entry['digest'] != rec.digest or entry['status'] != 'active':
            return False
        if rec.corrects_key and cursor['corrections'].get(rec.key) != rec.corrects_key:
            return False
    return True


# ---------------------------------------------------------------- 轮次收尾

def close_scan(state, scan_id, *, mode, complete):
    """结束一轮扫描。仅完整全量扫描才淘汰消失记录，审计历史保留。"""
    scan = state['scans'].setdefault(scan_id, _new_scan())
    scan['complete'] = complete
    if scan['closed']:
        return {'scan': scan_id, 'skipped': True, 'reason': 'scan-closed',
                'cursor': cursor_snapshot(state)}
    scan['closed'] = True
    retired = []
    if mode == 'full' and complete:
        seen = set(scan['seen'])
        for key in sorted(state['index']):
            entry = state['index'][key]
            if entry['status'] == 'active' and key not in seen:
                entry['status'] = 'retired'
                entry['retired_scan'] = scan_id
                _emit(state, {'id': f'retire:{key}:{scan_id}', 'kind': 'retire',
                              'key': key, 'scan': scan_id})
                retired.append(key)
    state['cursor']['scan'] = None
    state['cursor']['mode'] = mode
    return {'scan': scan_id, 'skipped': False, 'mode': mode, 'complete': complete,
            'seen': len(scan['seen']), 'retired': retired,
            'cursor': cursor_snapshot(state)}


def replay_round(state, round_dict, *, mode, fail_at=None, on_commit=None):
    """按轮次重放分页响应；``fail_at=(scan, page)`` 在该页前注入故障。"""
    if mode not in MODES:
        raise ValueError(f'unknown mode: {mode!r}')
    scan_id = round_dict['scan']
    complete = bool(round_dict.get('complete'))
    scan = state['scans'].setdefault(scan_id, _new_scan())
    scan['complete'] = complete
    if scan['closed']:
        return {'scan': scan_id, 'mode': mode, 'skipped': True, 'pages': [],
                'close': None, 'cursor': cursor_snapshot(state)}
    page_diffs = []
    for page_index, rows in enumerate(round_dict.get('pages', [])):
        if fail_at is not None and tuple(fail_at) == (scan_id, page_index):
            raise SimulatedCrash(scan_id, page_index)
        already = scan['pages'].get(str(page_index)) == _page_hash(scan_id, page_index, rows)
        if not already and mode == 'incremental' and _page_is_stale(state, rows):
            scan['cutoff'] = True
            page_diffs.append({'scan': scan_id, 'page': page_index, 'skipped': True,
                               'reason': 'incremental-cutoff', 'rows': len(rows),
                               'cursor': cursor_snapshot(state)})
            break
        diff = apply_page(state, scan_id, page_index, rows, mode=mode)
        page_diffs.append(diff)
        if on_commit is not None and not diff['skipped']:
            on_commit(state)
    close = close_scan(state, scan_id, mode=mode, complete=complete)
    if on_commit is not None:
        on_commit(state)
    return {'scan': scan_id, 'mode': mode, 'skipped': False, 'pages': page_diffs,
            'close': close, 'cursor': cursor_snapshot(state)}


# ---------------------------------------------------------------- 查询

def lineage(state, key):
    """更正前后关联：被谁更正、更正了谁、双向链条与原文摘要。"""
    edges = state['cursor']['corrections']
    back, seen = [], {key}
    current = edges.get(key)
    while current is not None and current not in seen:
        seen.add(current)
        back.append(current)
        current = edges.get(current)
    reverse = {}
    for source, target in edges.items():
        reverse.setdefault(target, []).append(source)
    forward, frontier, seen = [], list(reverse.get(key, [])), {key}
    while frontier:
        node = frontier.pop(0)
        if node in seen:
            continue
        seen.add(node)
        forward.append(node)
        frontier.extend(reverse.get(node, []))
    return {
        'key': key,
        'corrects': edges.get(key),
        'corrected_by': sorted(reverse.get(key, [])),
        'chain_back': back,
        'chain_forward': sorted(forward),
        'current': state['index'].get(key),
        'audit': [entry for entry in state['audit'] if entry['key'] == key],
    }


def status_report(state):
    index = {}
    for key, entry in sorted(state['index'].items()):
        view = {'status': entry['status'], 'digest': entry['digest'],
                'first_scan': entry['first_scan'], 'last_scan': entry['last_scan']}
        if 'retired_scan' in entry:
            view['retired_scan'] = entry['retired_scan']
        index[key] = view
    return {
        'cursor': cursor_snapshot(state),
        'index': index,
        'audit_entries': len(state['audit']),
        'outbox': list(state['outbox']),
        'scans': {sid: {'complete': scan['complete'], 'closed': scan['closed'],
                        'cutoff': scan['cutoff'], 'pages_committed': len(scan['pages']),
                        'seen': len(scan['seen'])}
                  for sid, scan in sorted(state['scans'].items())},
    }


# ---------------------------------------------------------------- 命令行

def _cmd_replay(args):
    fail_at = None
    if args.fail_at:
        scan, _, page = args.fail_at.rpartition(':')
        fail_at = (scan, int(page))
    rounds = json.loads(Path(args.fixtures).read_text(encoding='utf-8'))['rounds']
    if args.rounds:
        wanted = set(args.rounds.split(','))
        rounds = [item for item in rounds if item['scan'] in wanted]
    state = load_state(args.state)
    reports = []
    try:
        for round_dict in rounds:
            reports.append(replay_round(
                state, round_dict, mode=args.mode, fail_at=fail_at,
                on_commit=lambda committed: save_state(args.state, committed)))
    except SimulatedCrash as exc:
        save_state(args.state, state)
        print(json.dumps({
            'error': {'kind': 'simulated-crash', 'scan': exc.scan, 'page': exc.page},
            'rounds': reports,
            'cursor': cursor_snapshot(state),
        }, ensure_ascii=False, indent=2))
        return 3
    save_state(args.state, state)
    print(json.dumps({
        'mode': args.mode,
        'rounds': reports,
        'cursor': cursor_snapshot(state),
        'outbox': state['outbox'],
    }, ensure_ascii=False, indent=2))
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='公告采集工作流：重放分页响应，报告可核对的游标与差异')
    commands = parser.add_subparsers(dest='command', required=True)

    replay = commands.add_parser('replay', help='按轮次重放分页响应')
    replay.add_argument('--fixtures', default='fixtures/scans.json',
                        help='分页响应记录（默认 fixtures/scans.json）')
    replay.add_argument('--state', required=True, help='状态文件路径（JSON）')
    replay.add_argument('--mode', choices=MODES, default='full',
                        help='full=全量扫描（完整轮次结束后淘汰消失索引）；'
                             'incremental=增量扫描（游标覆盖处截断，不淘汰）')
    replay.add_argument('--rounds', default=None, help='只重放指定轮次，逗号分隔')
    replay.add_argument('--fail-at', default=None, metavar='SCAN:PAGE',
                        help='在该页之前注入故障（退出码 3），用于验证断点续跑')

    status = commands.add_parser('status', help='打印游标、当前索引与下游发送记录')
    status.add_argument('--state', required=True)

    trace = commands.add_parser('lineage', help='查询公告的更正前后关联与原文摘要')
    trace.add_argument('--state', required=True)
    trace.add_argument('--key', required=True, help='复合键，如 a.example:7')

    args = parser.parse_args(argv)
    if args.command == 'replay':
        return _cmd_replay(args)
    if args.command == 'status':
        print(json.dumps(status_report(load_state(args.state)),
                         ensure_ascii=False, indent=2))
        return 0
    if args.command == 'lineage':
        print(json.dumps(lineage(load_state(args.state), args.key),
                         ensure_ascii=False, indent=2))
        return 0
    return 2


if __name__ == '__main__':
    sys.exit(main())
