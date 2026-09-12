# ============================================================
# FILE: audit_protocol.py
# ROLE: Audit script verifying developer protocol adherence across codebase.
# ============================================================
import os, re

py_files = []
for root, dirs, files in os.walk('.'):
    if any(x in root for x in ['.git', '__pycache__', '.venv', 'venv', 'scratch']):
        continue
    for f in files:
        if f.endswith('.py'):
            py_files.append(os.path.normpath(os.path.join(root, f)))

print(f"Auditing {len(py_files)} Python files...")

cfg_get_matches = []
for p in py_files:
    if 'live_tests' in p:
        continue # Focus on production code first
    lines = open(p, encoding='utf-8', errors='ignore').readlines()
    for i, line in enumerate(lines):
        if re.search(r'(cfg|config)\["[^"]+"\]\.get\(', line) or re.search(r'(cfg|config)\.get\(', line) or re.search(r'self\.cfg\["[^"]+"\]\.get\(', line):
            cfg_get_matches.append((p, i+1, line.strip()))

print(f"\nProduction matches for cfg.get or cfg['...'].get: {len(cfg_get_matches)}")
for p, ln, l in cfg_get_matches:
    print(f"  {p}:{ln} -> {l}")
