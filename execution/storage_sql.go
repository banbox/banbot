package execution

// storageOperation names a fixed execution record access, never a SQL string.
// Only the SQLite adapter owns statements; MemoryStore operates on records.
type storageOperation uint8

const (
	opListRecoveryOrders          storageOperation = 106
	opReadLotEntryStepsBuy        storageOperation = 104
	opReadLotEntryStepsSell       storageOperation = 105
	opListStrategyCheckpointPage  storageOperation = 103
	opReadLotEntryBuy             storageOperation = 101
	opReadLotEntrySell            storageOperation = 102
	opListAttemptEvents           storageOperation = 100
	opPutStrategyCheckpoint       storageOperation = 0
	opReadIntentRuntime           storageOperation = 1
	opReadPlan                    storageOperation = 2
	opReadLatestSequence          storageOperation = 3
	opReadAccountFrozen           storageOperation = 4
	opCountUncertainOrders        storageOperation = 5
	opUpdateOrderAttempt          storageOperation = 6
	opUpdateExchangeID            storageOperation = 9
	opReadOrderReportMode         storageOperation = 11
	opFindVenueOrder              storageOperation = 12
	opReadStrategyTotals          storageOperation = 13
	opReadLatestPlan              storageOperation = 14
	opReadIntentFilled            storageOperation = 15
	opReadIntentReserved          storageOperation = 16
	opUpdateIntentRuntime         storageOperation = 17
	opReadIntentConsumed          storageOperation = 18
	opReadLot                     storageOperation = 19
	opReadStrategyCheckpoint      storageOperation = 20
	opReadEvent                   storageOperation = 21
	opInsertEvent                 storageOperation = 22
	opInsertPosting               storageOperation = 23
	opReadUnassignedCash          storageOperation = 24
	opUpdateUnassignedCash        storageOperation = 25
	opReadStrategyCash            storageOperation = 26
	opPutStrategyCash             storageOperation = 27
	opEnsureStrategy              storageOperation = 28
	opUpdateStrategyTotals        storageOperation = 29
	opReadAccountCash             storageOperation = 30
	opUpdateAccountCash           storageOperation = 31
	opUpdateAccountFrozen         storageOperation = 32
	opCheckpointEvent             storageOperation = 33
	opReadPnLReclassification     storageOperation = 34
	opUpdatePnLReclassification   storageOperation = 35
	opPutLot                      storageOperation = 36
	opReadActualPosition          storageOperation = 37
	opPutActualPosition           storageOperation = 38
	opUpdateReportMode            storageOperation = 39
	opReadAllocationFilled        storageOperation = 40
	opIncrementAllocationFilled   storageOperation = 41
	opUpdateFillHighwater         storageOperation = 42
	opUpdateOrderFee              storageOperation = 43
	opReadAccountSnapshot         storageOperation = 44
	opListStrategyCash            storageOperation = 45
	opListActiveLots              storageOperation = 46
	opListActiveOrders            storageOperation = 47
	opListActualPositions         storageOperation = 48
	opListExternalPositions       storageOperation = 49
	opReadMigration               storageOperation = 50
	opReadLegacySource            storageOperation = 51
	opReadMigrationHash           storageOperation = 52
	opCountMigrations             storageOperation = 53
	opInsertMigration             storageOperation = 54
	opFreezeAccount               storageOperation = 55
	opReadMigrationState          storageOperation = 56
	opCountMigrationDestination   storageOperation = 57
	opReadAccountGenesis          storageOperation = 58
	opInsertStrategy              storageOperation = 59
	opInsertLot                   storageOperation = 60
	opInsertActualPosition        storageOperation = 61
	opInsertLegacySource          storageOperation = 62
	opUpdateAccountGenesis        storageOperation = 63
	opFinishMigration             storageOperation = 64
	opReadPlanIntentDefinition    storageOperation = 65
	opReadAllocatedSteps          storageOperation = 66
	opInsertImportedOrder         storageOperation = 67
	opInsertImportedAllocation    storageOperation = 68
	opListCommittedEvents         storageOperation = 69
	opListEventPostings           storageOperation = 70
	opReadProjection              storageOperation = 71
	opReadAccountCheckpoint       storageOperation = 72
	opPutProjection               storageOperation = 73
	opListPlanOrders              storageOperation = 74
	opReadPlanIntentRuntime       storageOperation = 75
	opInsertInternalAllocation    storageOperation = 76
	opReadExternalPosition        storageOperation = 77
	opPutExternalPosition         storageOperation = 78
	opListReconciliationPositions storageOperation = 79
	opCountPendingMigrations      storageOperation = 80
	opCountExternalPositions      storageOperation = 81
	opReadPlanSequence            storageOperation = 84
	opListOrdersByState           storageOperation = 85
	opInsertPlan                  storageOperation = 86
	opReadIntentDefinition        storageOperation = 87
	opEnsureIntent                storageOperation = 88
	opInsertPlanMembership        storageOperation = 89
	opReadOrderDefinition         storageOperation = 90
	opInsertOrder                 storageOperation = 91
	opInsertAllocation            storageOperation = 92
	opReadOrder                   storageOperation = 93
	opUpdateOrderState            storageOperation = 94
)

var sqliteStatements = map[storageOperation]string{
	opListRecoveryOrders:          "SELECT id FROM exec_order WHERE account=? AND id>? AND (attempt>0 OR exchange_id<>'') ORDER BY id LIMIT ?",
	opReadLotEntryStepsBuy:        "SELECT COALESCE(sum(quantity),0) FROM exec_ledger WHERE account=? AND strategy=? AND lot=? AND kind IN ('ExchangeFill','InternalFill') AND quantity>0",
	opReadLotEntryStepsSell:       "SELECT COALESCE(sum(-quantity),0) FROM exec_ledger WHERE account=? AND strategy=? AND lot=? AND kind IN ('ExchangeFill','InternalFill') AND quantity<0",
	opListStrategyCheckpointPage:  "SELECT name,payload FROM exec_checkpoint WHERE account=?1 AND kind='strategy' AND strategy=?2 AND name>?4 AND name>=?3 AND substr(name,1,length(?3))=?3 ORDER BY name LIMIT ?5",
	opReadLotEntryBuy:             "SELECT COALESCE(max(at_ms),0) FROM exec_ledger WHERE account=? AND strategy=? AND lot=? AND kind IN ('ExchangeFill','InternalFill') AND quantity>0",
	opReadLotEntrySell:            "SELECT COALESCE(max(at_ms),0) FROM exec_ledger WHERE account=? AND strategy=? AND lot=? AND kind IN ('ExchangeFill','InternalFill') AND quantity<0",
	opListAttemptEvents:           "SELECT payload FROM exec_event WHERE account=? AND kind='OrderAttempt' AND json_extract(payload,'$.OrderID')=? ORDER BY checkpoint",
	opPutStrategyCheckpoint:       "INSERT INTO exec_checkpoint(account,kind,strategy,name,payload) VALUES(?,'strategy',?,?,?) ON CONFLICT(account,kind,strategy,name) DO UPDATE SET payload=excluded.payload",
	opReadIntentRuntime:           "SELECT COALESCE(runtime_payload,payload) FROM exec_virtual_intent WHERE account=? AND id=?",
	opReadPlan:                    "SELECT payload FROM exec_plan WHERE account=? AND id=?",
	opReadLatestSequence:          "SELECT max(sequence) FROM exec_plan WHERE account=?",
	opReadAccountFrozen:           "SELECT frozen FROM exec_account WHERE account=?",
	opCountUncertainOrders:        "SELECT count(*) FROM exec_order WHERE account=? AND state IN (?,?,?)",
	opUpdateOrderAttempt:          "UPDATE exec_order SET attempt=?,generation=? WHERE account=? AND id=?",
	opUpdateExchangeID:            "UPDATE exec_order SET exchange_id=? WHERE account=? AND id=?",
	opReadOrderReportMode:         "SELECT report_mode FROM exec_order WHERE account=? AND id=?",
	opFindVenueOrder:              "SELECT id FROM exec_order WHERE account=? AND ((?<>'' AND client_id=?) OR (?<>'' AND exchange_id=?))",
	opReadStrategyTotals:          "SELECT fees,funding FROM exec_strategy WHERE account=? AND strategy=?",
	opReadLatestPlan:              "SELECT payload FROM exec_plan WHERE account=? ORDER BY sequence DESC LIMIT 1",
	opReadIntentFilled:            "SELECT COALESCE((SELECT sum(filled) FROM exec_allocation WHERE account=? AND intent_id=?),0)+COALESCE((SELECT sum(steps) FROM exec_internal_allocation WHERE account=? AND intent_id=?),0)",
	opReadIntentReserved:          "SELECT COALESCE(sum(a.steps-a.filled),0) FROM exec_allocation a JOIN exec_order o ON o.account=a.account AND o.id=a.order_id WHERE a.account=? AND a.intent_id=? AND o.state NOT IN ('Canceled','Rejected','Filled')",
	opUpdateIntentRuntime:         "UPDATE exec_virtual_intent SET runtime_payload=? WHERE account=? AND id=?",
	opReadIntentConsumed:          "SELECT COALESCE((SELECT sum(CASE WHEN o.state IN ('Canceled','Rejected') THEN a.filled ELSE a.steps END) FROM exec_allocation a JOIN exec_order o ON o.account=a.account AND o.id=a.order_id WHERE a.account=? AND a.intent_id=?),0)+COALESCE((SELECT sum(steps) FROM exec_internal_allocation WHERE account=? AND intent_id=?),0)",
	opReadLot:                     "SELECT payload FROM exec_lot WHERE account=? AND strategy=? AND id=?",
	opReadStrategyCheckpoint:      "SELECT payload FROM exec_checkpoint WHERE account=? AND kind='strategy' AND strategy=? AND name=?",
	opReadEvent:                   "SELECT kind,payload FROM exec_event WHERE account=? AND id=?",
	opInsertEvent:                 "INSERT INTO exec_event(account,id,kind,payload) VALUES(?,?,?,?)",
	opInsertPosting:               "INSERT INTO exec_ledger(account,event_id,kind,strategy,lot,quantity,cash,fee,realized,at_ms) VALUES(?,?,?,?,?,?,?,?,?,?)",
	opReadUnassignedCash:          "SELECT unassigned FROM exec_account WHERE account=?",
	opUpdateUnassignedCash:        "UPDATE exec_account SET unassigned=? WHERE account=?",
	opReadStrategyCash:            "SELECT cash FROM exec_strategy WHERE account=? AND strategy=?",
	opPutStrategyCash:             "INSERT INTO exec_strategy(account,strategy,cash) VALUES(?,?,?) ON CONFLICT(account,strategy) DO UPDATE SET cash=excluded.cash",
	opEnsureStrategy:              "INSERT OR IGNORE INTO exec_strategy(account,strategy,cash) VALUES(?,?,?)",
	opUpdateStrategyTotals:        "UPDATE exec_strategy SET fees=?,funding=? WHERE account=? AND strategy=?",
	opReadAccountCash:             "SELECT cash FROM exec_account WHERE account=?",
	opUpdateAccountCash:           "UPDATE exec_account SET cash=?,checkpoint=checkpoint+1 WHERE account=?",
	opUpdateAccountFrozen:         "UPDATE exec_account SET frozen=? WHERE account=?",
	opCheckpointEvent:             "UPDATE exec_event SET checkpoint=(SELECT checkpoint FROM exec_account WHERE account=?) WHERE account=? AND id=?",
	opReadPnLReclassification:     "SELECT pnl_reclassification FROM exec_account WHERE account=?",
	opUpdatePnLReclassification:   "UPDATE exec_account SET pnl_reclassification=? WHERE account=?",
	opPutLot:                      "INSERT INTO exec_lot(account,strategy,id,quantity,payload) VALUES(?,?,?,?,?) ON CONFLICT(account,strategy,id) DO UPDATE SET quantity=excluded.quantity,payload=excluded.payload",
	opReadActualPosition:          "SELECT payload FROM exec_position WHERE account=? AND instrument=?",
	opPutActualPosition:           "INSERT INTO exec_position(account,instrument,payload) VALUES(?,?,?) ON CONFLICT(account,instrument) DO UPDATE SET payload=excluded.payload",
	opUpdateReportMode:            "UPDATE exec_order SET report_mode=? WHERE account=? AND id=?",
	opReadAllocationFilled:        "SELECT filled FROM exec_allocation WHERE account=? AND order_id=? AND id=?",
	opIncrementAllocationFilled:   "UPDATE exec_allocation SET filled=filled+? WHERE account=? AND order_id=? AND id=?",
	opUpdateFillHighwater:         "UPDATE exec_order SET filled=?,fee=?,cost=?,report_mode=? WHERE account=? AND id=?",
	opUpdateOrderFee:              "UPDATE exec_order SET fee=? WHERE account=? AND id=?",
	opReadAccountSnapshot:         "SELECT cash,unassigned,pnl_reclassification,frozen,checkpoint FROM exec_account WHERE account=?",
	opListStrategyCash:            "SELECT strategy,cash FROM exec_strategy WHERE account=? ORDER BY strategy",
	opListActiveLots:              "SELECT payload FROM exec_lot WHERE account=? AND quantity<>0 ORDER BY strategy,id",
	opListActiveOrders:            "SELECT id FROM exec_order WHERE account=? AND state IN ('Prepared','Sending','Unknown','Acknowledged','Partial','CancelPending') ORDER BY id",
	opListActualPositions:         "SELECT payload FROM exec_position WHERE account=? ORDER BY instrument",
	opListExternalPositions:       "SELECT payload FROM exec_external_position WHERE account=? ORDER BY instrument",
	opReadMigration:               "SELECT id,source_hash,source_version,schema_version,state FROM exec_migration WHERE account=? AND id=?",
	opReadLegacySource:            "SELECT payload FROM exec_legacy_source WHERE account=? AND strategy=? AND lot=?",
	opReadMigrationHash:           "SELECT source_hash,state FROM exec_migration WHERE account=? AND id=?",
	opCountMigrations:             "SELECT count(*) FROM exec_migration WHERE account=?",
	opInsertMigration:             "INSERT INTO exec_migration(account,id,source_hash,source_version,schema_version,state,payload) VALUES(?,?,?,?,?,'pending',?)",
	opFreezeAccount:               "UPDATE exec_account SET frozen=1 WHERE account=?",
	opReadMigrationState:          "SELECT state,source_hash FROM exec_migration WHERE account=? AND id=?",
	opCountMigrationDestination:   "SELECT (SELECT count(*) FROM exec_plan WHERE account=?)+(SELECT count(*) FROM exec_lot WHERE account=?)+(SELECT count(*) FROM exec_position WHERE account=?)+(SELECT count(*) FROM exec_strategy WHERE account=?)+(SELECT count(*) FROM exec_event WHERE account=?)+(SELECT count(*) FROM exec_checkpoint WHERE account=? AND kind='strategy' AND NOT(strategy='account-risk' AND name='policy-v1'))+(SELECT count(*) FROM exec_external_position WHERE account=?)",
	opReadAccountGenesis:          "SELECT cash,unassigned,pnl_reclassification,checkpoint FROM exec_account WHERE account=?",
	opInsertStrategy:              "INSERT INTO exec_strategy(account,strategy,cash) VALUES(?,?,?)",
	opInsertLot:                   "INSERT INTO exec_lot(account,strategy,id,quantity,payload) VALUES(?,?,?,?,?)",
	opInsertActualPosition:        "INSERT INTO exec_position(account,instrument,payload) VALUES(?,?,?)",
	opInsertLegacySource:          "INSERT INTO exec_legacy_source(account,migration_id,strategy,lot,payload) VALUES(?,?,?,?,?)",
	opUpdateAccountGenesis:        "UPDATE exec_account SET cash=?,unassigned=?,pnl_reclassification=? WHERE account=?",
	opFinishMigration:             "UPDATE exec_migration SET state='ready' WHERE account=? AND id=?",
	opReadPlanIntentDefinition:    "SELECT v.payload FROM exec_virtual_intent v JOIN exec_plan_intent p ON p.account=v.account AND p.intent_id=v.id WHERE v.account=? AND v.id=? AND p.plan_id=?",
	opReadAllocatedSteps:          "SELECT COALESCE(sum(steps),0) FROM exec_allocation WHERE account=? AND intent_id=?",
	opInsertImportedOrder:         "INSERT INTO exec_order(account,id,plan_id,payload,client_id,exchange_id,state,filled,fee,cost,report_mode,attempt,generation) VALUES(?,?,?,?,?,?,?,?,?,?,'cumulative',?,?)",
	opInsertImportedAllocation:    "INSERT INTO exec_allocation(account,order_id,id,intent_id,strategy,lot,steps,filled) VALUES(?,?,?,?,?,?,?,?)",
	opListCommittedEvents:         "SELECT id,kind,checkpoint,payload FROM exec_event WHERE account=? AND checkpoint>? ORDER BY checkpoint LIMIT ?",
	opListEventPostings:           "SELECT id,kind,strategy,lot,quantity,cash,fee,realized,at_ms FROM exec_ledger WHERE account=? AND event_id=? ORDER BY id",
	opReadProjection:              "SELECT checkpoint FROM exec_checkpoint WHERE account=? AND kind='projection' AND strategy='' AND name=?",
	opReadAccountCheckpoint:       "SELECT checkpoint FROM exec_account WHERE account=?",
	opPutProjection:               "INSERT INTO exec_checkpoint(account,kind,strategy,name,checkpoint) VALUES(?,'projection','',?,?) ON CONFLICT(account,kind,strategy,name) DO UPDATE SET checkpoint=excluded.checkpoint",
	opListPlanOrders:              "SELECT id FROM exec_order WHERE account=? AND plan_id=? ORDER BY id",
	opReadPlanIntentRuntime:       "SELECT COALESCE(runtime_payload,payload) FROM exec_virtual_intent WHERE account=? AND id=? AND EXISTS(SELECT 1 FROM exec_plan_intent WHERE account=? AND plan_id=? AND intent_id=?)",
	opInsertInternalAllocation:    "INSERT INTO exec_internal_allocation(account,event_id,intent_id,steps) VALUES(?,?,?,?)",
	opReadExternalPosition:        "SELECT payload FROM exec_external_position WHERE account=? AND instrument=?",
	opPutExternalPosition:         "INSERT INTO exec_external_position(account,instrument,quantity,payload) VALUES(?,?,?,?) ON CONFLICT(account,instrument) DO UPDATE SET quantity=excluded.quantity,payload=excluded.payload",
	opListReconciliationPositions: "SELECT payload FROM exec_position WHERE account=?",
	opCountPendingMigrations:      "SELECT count(*) FROM exec_migration WHERE account=? AND state='pending'",
	opCountExternalPositions:      "SELECT count(*) FROM exec_external_position WHERE account=? AND quantity<>0",
	opReadPlanSequence:            "SELECT COALESCE(max(sequence),-1) FROM exec_plan WHERE account=?",
	opListOrdersByState:           "SELECT id FROM exec_order WHERE account=? AND state=?",
	opInsertPlan:                  "INSERT INTO exec_plan(account,id,sequence,payload) VALUES(?,?,?,?)",
	opReadIntentDefinition:        "SELECT payload FROM exec_virtual_intent WHERE account=? AND id=?",
	opEnsureIntent:                "INSERT OR IGNORE INTO exec_virtual_intent(account,id,plan_id,payload,runtime_payload) VALUES(?,?,?,?,?)",
	opInsertPlanMembership:        "INSERT INTO exec_plan_intent(account,plan_id,intent_id) VALUES(?,?,?)",
	opReadOrderDefinition:         "SELECT payload FROM exec_order WHERE account=? AND id=?",
	opInsertOrder:                 "INSERT INTO exec_order(account,id,plan_id,payload,client_id,state) VALUES(?,?,?,?,?,?)",
	opInsertAllocation:            "INSERT INTO exec_allocation(account,order_id,id,intent_id,strategy,lot,steps) VALUES(?,?,?,?,?,?,?)",
	opReadOrder:                   "SELECT payload,client_id,exchange_id,state,filled,fee,cost,attempt,generation FROM exec_order WHERE account=? AND id=?",
	opUpdateOrderState:            "UPDATE exec_order SET state=? WHERE account=? AND id=?",
}
