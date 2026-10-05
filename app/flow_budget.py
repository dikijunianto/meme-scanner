"""Shared flow limits; a budget deferral is never an RPC/connection failure."""


class FlowBudget(Exception):
    def __init__(self, scope, used=None, limit=None, reset_at=None, first_block=None):
        self.scope,self.used,self.limit,self.reset_at,self.first_block=scope,used,limit,reset_at,first_block
        self.category=('UNRECOVERABLE_GAP' if scope=='recovery_range' else
                       'TEMPORARY_BUDGET_WAIT' if scope=='minute_rpc' else
                       'DAILY_BUDGET_EXHAUSTED' if scope in ('daily_rpc','daily_getlogs') else
                       'RESOURCE_LIMIT')
        super().__init__(scope)


class BudgetWait(FlowBudget):
    """A limiter rejected before send; preserve the obligation and retry later."""
