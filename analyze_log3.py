import re

first_run_codes = set()
second_run_codes = set()
first_run_zero = set()
second_run_zero = set()

in_range = False
run = 0

with open(r'c:\PycharmProjects\quant_data_center\logs\qdc.log', 'r', encoding='utf-8') as f:
    for line in f:
        if 'hist update started' in line and 'adjust=all' in line:
            run += 1
            in_range = True
            continue
        if in_range and 'hist update completed' in line:
            in_range = False
            continue
        if in_range and 'hist progress' in line:
            m_code = re.search(r'code=(\S+)', line)
            m_adj = re.search(r'adjust=(\S+)', line)
            m_rows = re.search(r'rows=(\d+)', line)
            if m_code and m_adj:
                key = (m_code.group(1), m_adj.group(1))
                rows = int(m_rows.group(1)) if m_rows else -1
                if run == 1:
                    first_run_codes.add(key)
                    if rows == 0:
                        first_run_zero.add(key)
                elif run == 2:
                    second_run_codes.add(key)
                    if rows == 0:
                        second_run_zero.add(key)

print(f'First full run (adjust=all): {len(first_run_codes)} unique (code, adjust) pairs, {len(first_run_zero)} with rows=0')
print(f'Second full run (adjust=all): {len(second_run_codes)} unique (code, adjust) pairs, {len(second_run_zero)} with rows=0')

overlap = first_run_codes & second_run_codes
print(f'\nOverlap (processed in BOTH runs): {len(overlap)}')

only_first = first_run_codes - second_run_codes
only_second = second_run_codes - first_run_codes
print(f'Only in first run: {len(only_first)}')
print(f'Only in second run: {len(only_second)}')

print(f'\n--- Overlap analysis ---')
overlap_zero = overlap & first_run_zero
overlap_nonzero = overlap - first_run_zero
print(f'Overlap with rows=0 in first run: {len(overlap_zero)}')
print(f'Overlap with rows>0 in first run: {len(overlap_nonzero)}')

overlap_codes = set(c for c, a in overlap)
print(f'Unique codes in overlap: {len(overlap_codes)}')

first_run_codes_only = set(c for c, a in first_run_codes)
second_run_codes_only = set(c for c, a in second_run_codes)
print(f'Unique codes in first run: {len(first_run_codes_only)}')
print(f'Unique codes in second run: {len(second_run_codes_only)}')

overlap_code_set = first_run_codes_only & second_run_codes_only
print(f'Unique codes in both runs: {len(overlap_code_set)}')

only_second_codes = second_run_codes_only - first_run_codes_only
print(f'\nCodes only in second run (not in first): {len(only_second_codes)}')
if only_second_codes:
    for c in sorted(only_second_codes)[:30]:
        print(f'  {c}')

print(f'\n--- Sample overlap codes with rows>0 in first run ---')
sample = sorted(overlap_nonzero)[:20]
for c, a in sample:
    print(f'  code={c} adjust={a}')
