import json
from pathlib import Path
from service import notice_key


def run():
    scans = json.loads((Path(__file__).parent / 'fixtures/scans.json').read_text())
    first = scans['rounds'][0]
    keys = [notice_key(row['host'], row['id']) for page in first['pages'] for row in page]
    assert len(set(keys)) == 2
    return {'first_scan_keys': keys, 'available_rounds': [scan['scan'] for scan in scans['rounds']]}


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
