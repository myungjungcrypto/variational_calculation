#!/usr/bin/env python3
"""Regenerate the entire local analysis and RESULTS.md with no network access."""
import hashlib
import json
import subprocess
import sys
from core import HERE, DATA, load, OI
import model
import costs
import platform_points
import economics
import report
import verify
import tvl_sensitivity
import counterparty_audit


def main():
    files=sorted(p for p in DATA.iterdir() if p.is_file())
    manifest=[{'path':str(p.relative_to(HERE.parent)), 'bytes':p.stat().st_size,
               'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in files]
    (HERE/'input_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print('Reconstructing positions and integrating OI from local exports...',flush=True)
    trades=load()
    model.run(trades,OI(trades))
    costs.main()
    counterparty_audit.run()
    platform_points.run()
    subprocess.run([sys.executable,str(HERE/'independent_audit.py')],check=True)
    economics.run()
    tvl_sensitivity.run()
    report.run()
    verify.run()
    for item in manifest:
        source=HERE.parent/item['path']
        assert hashlib.sha256(source.read_bytes()).hexdigest()==item['sha256'],'Input changed during analysis'
    print('Wrote '+str(HERE.parent/'RESULTS.md'),flush=True)


if __name__=='__main__':
    main()
