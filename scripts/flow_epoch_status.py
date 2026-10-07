"""Read-only current-epoch telemetry, with separately retained historical debt."""
if __package__:from scripts import _bootstrap  # noqa: F401
else:import _bootstrap  # noqa: F401
import json
import time
from app.config import Config
from app.flow_data import FlowDB
from app.flow_epochs import historical_debt
from app.flow_cutover import gap_counts,session
from app.flow_provider_switch import report
from app.flow_worker import FlowSettings
from scripts.phase2c_dataset_audit import open_readonly
from scripts.phase2c_point_in_time_audit import ledger_audit


def status():
    settings=FlowSettings.load();db=FlowDB(settings.database,readonly=True)
    try:
        cutover=session(db)
        from app.flow_segments import record,historical_debt as preclean_debt
        segment=record(db)
        from app.flow_partition_schema import missing,VERSION
        absent=missing(db.conn) if segment else []
        with open_readonly(Config.load().database,settings.database) as joined:
            maturity=ledger_audit(joined,time.time())
        return {'current_epoch':db.epoch(),'health':db.current_health(),
                'partition_schema':{'contract_version':VERSION,'missing':absent,'complete':not absent},
                'research_segment':segment,'preclean_incident_debt':preclean_debt(db),
                'research_clean_start':segment['start_at'] if segment and segment['status']=='VALIDATED' else None,
                'active_and_historical_current_epoch_gaps':gap_counts(db),
                'current_epoch_switches':report(db,session_id=cutover['id'] if cutover else None),
                'current_epoch_pit':maturity,'historical_quarantined_debt':historical_debt(db),
                'shared_budget_today':{m:db.used(m,int(time.time())//86400*86400)
                    for m in ('flow_eth_getLogs','flow_rpc_members')},
                'historical_rows_policy':'descriptive/diagnostic/secondary only; never pooled into primary maturity'}
    finally:db.close()


if __name__=='__main__':print(json.dumps(status(),indent=2))
