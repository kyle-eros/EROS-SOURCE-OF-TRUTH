import json, sys, io

if len(sys.argv) != 4:
    print("Usage: python3 apply_mappings.py <mappings.json> <in.sql> <out.sql>")
    sys.exit(1)

mappings_path, in_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]

with io.open(mappings_path, 'r', encoding='utf-8') as f:
    mappings = json.load(f)

with io.open(in_path, 'r', encoding='utf-8') as f:
    sql = f.read()

for k in sorted(mappings.keys(), key=len, reverse=True):
    sql = sql.replace(k, mappings[k])

with io.open(out_path, 'w', encoding='utf-8') as f:
    f.write(sql)