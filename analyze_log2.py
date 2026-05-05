import re

in_range = False
codes_in_second_run = []
zero_rows_in_second_run = []

with open(r'c:\PycharmProjects\quant_data_center\logs\qdc.log', 'r', encoding='utf-8') as f:
    for line in f:
        if '2026-05-05 10:39:21.758' in line and 'hist update started' in line:
            in_range = True
            continue
        if in_range and 'hist update completed' in line:
            in_range = False
            break
        if in_range and 'hist progress' in line:
            m_code = re.search(r'code=(\S+)', line)
            m_adj = re.search(r'adjust=(\S+)', line)
            m_rows = re.search(r'rows=(\d+)', line)
            m_status = re.search(r'status=(\S+)', line)
            if m_code and m_adj:
                code = m_code.group(1)
                adj = m_adj.group(1)
                rows = int(m_rows.group(1)) if m_rows else -1
                status = m_status.group(1) if m_status else 'unknown'
                codes_in_second_run.append((code, adj, rows, status))
                if rows == 0:
                    zero_rows_in_second_run.append((code, adj))

print(f'Total tasks in second run: {len(codes_in_second_run)}')
print(f'Tasks with rows=0: {len(zero_rows_in_second_run)}')

zero_codes = set(c for c, a in zero_rows_in_second_run)
print(f'Unique codes with rows=0: {len(zero_codes)}')

by_adjust = {}
for c, a, rows, status in codes_in_second_run:
    by_adjust.setdefault(a, []).append((c, rows, status))

for a, items in sorted(by_adjust.items()):
    zero_count = sum(1 for _, r, _ in items if r == 0)
    success_count = sum(1 for _, _, s in items if s == 'success')
    failed_count = sum(1 for _, _, s in items if s == 'failed')
    print(f'\nadjust={a}: total={len(items)}, success={success_count}, failed={failed_count}, rows=0: {zero_count}')

print('\n--- First 30 tasks in second run ---')
for c, a, rows, status in codes_in_second_run[:30]:
    print(f'  code={c} adjust={a} rows={rows} status={status}')

print('\n--- Codes with rows=0 in second run ---')
for c, a in sorted(zero_rows_in_second_run):
    print(f'  code={c} adjust={a}')
