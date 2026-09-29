"""Check raw-fee totals independently and reconcile supplied position evidence."""
import csv
from decimal import Decimal
import gzip
import json
from pathlib import Path
import numpy as np
import pandas as pd
from core import HERE, DATA


def run():
    c=json.loads((HERE/'costs_summary.json').read_text())
    checks=[]
    for account in ('A1','A2','A3'):
        path=next(DATA.glob(account.lower()+'_lighter_export_*'))
        opening=gzip.open if path.suffix=='.gz' else open
        with opening(path,'rt') as stream:
            rows=list(csv.DictReader(stream))
        fees=sum((Decimal(row['Fee']) for row in rows),Decimal(0))
        volume=sum((Decimal(row['Trade Value']) for row in rows),Decimal(0))
        summary=next(x for x in c['window_summaries'] if x['account']==account and x['window']=='lifetime')
        assert abs(fees-Decimal(str(summary['fees_usd'])))<Decimal('0.0000001')
        assert abs(volume-Decimal(str(summary['volume_usd'])))<Decimal('0.0000001')
        assert len(rows)==summary['fills']
        checks.append({'check':account+' independent Decimal fee/volume totals','status':'pass','fills':len(rows)})
    p=pd.read_csv(HERE/'position_validation.csv')
    assert len(p)==len(pd.read_csv(DATA/'positions_snapshots.csv'))
    assert np.max(np.abs(p.size_difference))<1e-7
    checks.append({'check':'all supplied position sizes agree with reconstructed signed sizes','status':'pass','positions':len(p)})
    windows=pd.read_csv(HERE/'window_decomposition.csv')
    snapshots=pd.read_csv(DATA/'points_snapshots.csv')
    assert len(windows)==len(snapshots)-snapshots.account.nunique()
    for a in ('A1','A2','A3'):
        s=snapshots[snapshots.account==a].sort_values('timestamp_utc')
        got=windows[windows.account==a].delta_points.sum()
        assert abs(got-(s.points.iloc[-1]-s.points.iloc[0]))<1e-8
    checks.append({'check':'every adjacent snapshot interval represented; deltas telescope to raw observations','status':'pass','windows':len(windows)})
    apr=pd.read_csv(HERE/'apr_scenarios.csv')
    for case,g in apr.groupby('rate_case'):
        assert len(g)==72 and not g.duplicated(['lit_price_usd','program_end_points','gross_all_venues_oi_to_total_equity','structure']).any()
        assert set(g.lit_price_usd)=={3,4,5}
        assert set(g.program_end_points)=={1500000,2000000,3600000,5200000}
        assert set(g.gross_all_venues_oi_to_total_equity)=={10,15,20}
        assert set(g.structure)=={'A','B'}
    checks.append({'check':'all requested APR combinations are present for each rate scenario','status':'pass','rows':len(apr)})
    for path in HERE.glob('*.json'):
        json.loads(path.read_text(),parse_constant=lambda v: (_ for _ in ()).throw(ValueError('Invalid JSON '+v)))
    checks.append({'check':'JSON outputs contain no NaN or infinity tokens','status':'pass'})
    result={'checks':checks,'scope':'These are data and output checks, not validation that the points model is the true rule.'}
    (HERE/'verification.json').write_text(json.dumps(result,indent=2)+'\n')
    print(f"Verification: {len(checks)} checks passed.")
    return result


if __name__=='__main__':
    run()
