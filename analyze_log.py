import re

count = 0
zero_rows_codes = set()
all_codes = set()
in_range = False

with open(r'c:\PycharmProjects\quant_data_center\logs\qdc.log', 'r', encoding='utf-8') as f:
    for line in f:
        if '2026-05-04 22:13:44.798' in line and 'hist update started' in line:
            in_range = True
            continue
        if in_range and 'hist update completed' in line:
            in_range = False
            break
        if in_range and 'hist progress' in line:
            m_code = re.search(r'code=(\S+)', line)
            m_adj = re.search(r'adjust=(\S+)', line)
            m_rows = re.search(r'rows=(\d+)', line)
            if m_code and m_adj:
                key = (m_code.group(1), m_adj.group(1))
                all_codes.add(key)
                if m_rows and m_rows.group(1) == '0':
                    count += 1
                    zero_rows_codes.add(key)

print(f'Total rows=0 entries in last full run: {count}')
print(f'Unique (code, adjust) pairs with rows=0: {len(zero_rows_codes)}')
print(f'Total unique (code, adjust) pairs in last full run: {len(all_codes)}')
print()
print('Sample rows=0 codes:')
for c, a in sorted(zero_rows_codes)[:20]:
    print(f'  code={c} adjust={a}')

zero_codes_only = set(c for c, a in zero_rows_codes)
print(f'\nUnique codes with rows=0 (any adjust): {len(zero_codes_only)}')

zero_by_adjust = {}
for c, a in zero_rows_codes:
    zero_by_adjust.setdefault(a, set()).add(c)
for a, codes in sorted(zero_by_adjust.items()):
    print(f'  adjust={a}: {len(codes)} codes with rows=0')
