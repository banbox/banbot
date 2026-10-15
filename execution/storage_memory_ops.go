package execution

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
)

func (tx *storeTxn) memoryRead(operation storageOperation, args []any) ([][]any, error) {
	switch operation {
	case opListRecoveryOrders:
		account, after, limit := argString(args[0]), argString(args[1]), int(argInt(args[2]))
		var ids []string
		add := func(r memoryRecord) {
			if r.Account != account || r.ID <= after || r.Attempt == 0 && r.ExchangeId == "" {
				return
			}
			index := sort.SearchStrings(ids, r.ID)
			if index == limit {
				return
			}
			if len(ids) < limit {
				ids = append(ids, "")
			}
			copy(ids[index+1:], ids[index:])
			ids[index] = r.ID
		}
		for _, r := range tx.memory.records[recordsOrder] {
			add(r)
		}
		tx.visitHistory(recordsOrder, " AND k1>? AND (json_extract(body,'$.Attempt')>0 OR exchange<>'') ORDER BY k1 LIMIT ?", []any{after, limit}, add)
		rows := make([][]any, 0, len(ids))
		for _, id := range ids {
			rows = append(rows, []any{id})
		}
		return rows, nil
	case opListStrategyCheckpointPage:
		account, strategy, prefix, after, limit := argString(args[0]), argString(args[1]), argString(args[2]), argString(args[3]), int(argInt(args[4]))
		// Keep at most one page while merging staged hot and indexed cold rows.
		var records []memoryRecord
		add := func(r memoryRecord) {
			if r.Account != account || r.Kind != "strategy" || r.Strategy != strategy || r.Name <= after || !strings.HasPrefix(r.Name, prefix) {
				return
			}
			index := sort.Search(len(records), func(i int) bool { return records[i].Name >= r.Name })
			if index == limit {
				return
			}
			if len(records) < limit {
				records = append(records, memoryRecord{})
			}
			copy(records[index+1:], records[index:])
			records[index] = r
		}
		for _, r := range tx.memory.records[recordsCheckpoint] {
			add(r)
		}
		tx.visitHistory(recordsCheckpoint, " AND k1=? AND k2>? AND k2>=? AND substr(k2,1,length(?))=? ORDER BY k2 LIMIT ?", []any{"strategy:" + strategy, after, prefix, prefix, prefix, limit}, add)
		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Name, r.Payload})
		}
		return rows, nil
	case opReadLotEntryBuy, opReadLotEntrySell, opReadLotEntryStepsBuy, opReadLotEntryStepsSell:
		var atMS int64
		owner := memoryKey{argString(args[0]), argString(args[1]), argString(args[2])}
		observe := func(r memoryRecord) {
			if r.Kind != "ExchangeFill" && r.Kind != "InternalFill" {
				return
			}
			if (operation == opReadLotEntryBuy || operation == opReadLotEntryStepsBuy) && r.Quantity > 0 || (operation == opReadLotEntrySell || operation == opReadLotEntryStepsSell) && r.Quantity < 0 {
				if operation == opReadLotEntryBuy || operation == opReadLotEntrySell {
					atMS = max(atMS, r.AtMs)
				} else if tx.recordErr == nil {
					atMS, tx.recordErr = checkedSteps(atMS, absSteps(r.Quantity))
				}
			}
		}
		for _, key := range tx.memory.postingsByLot[owner] {
			observe(tx.memory.records[recordsLedger][key])
		}
		tx.visitHistory(recordsLedger, " AND strategy=? AND lot=? AND event_type IN ('ExchangeFill','InternalFill')", []any{owner.first, owner.second}, observe)
		return [][]any{{atMS}}, nil
	case opListAttemptEvents:
		records := tx.selectRecordsWhere(recordsEvent, " AND event_type=? AND order_id=?", []any{"OrderAttempt", argString(args[1])}, func(r memoryRecord) bool {
			return r.Account == argString(args[0]) && r.Kind == "OrderAttempt" && r.OrderId == argString(args[1])
		})
		sort.Slice(records, func(i, j int) bool { return records[i].Checkpoint < records[j].Checkpoint })
		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadIntentRuntime:
		records := tx.lookupRecords(recordsVirtualIntent, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.runtimeBody()})
		}
		return rows, nil
	case opReadPlan:
		records := tx.lookupRecords(recordsPlan, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadAccountFrozen:
		records := tx.lookupRecords(recordsAccount, memoryKey{argString(args[0]), "", ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Frozen})
		}
		return rows, nil
	case opCountUncertainOrders:
		records := tx.activeRecords(recordsOrder, tx.memory.activeOrders, func(r memoryRecord) bool {
			return r.Account == argString(args[0]) && (r.State == argString(args[1]) || r.State == argString(args[2]) || r.State == argString(args[3]))
		})

		return [][]any{{int64(len(records))}}, nil
	case opReadOrderReportMode:
		records := tx.lookupRecords(recordsOrder, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.ReportMode})
		}
		return rows, nil
	case opReadStrategyTotals:
		records := tx.lookupRecords(recordsStrategy, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Fees, r.Funding})
		}
		return rows, nil
	case opReadLot:
		records := tx.lookupRecords(recordsLot, memoryKey{argString(args[0]), argString(args[1]), argString(args[2])})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadStrategyCheckpoint:
		records := tx.lookupRecords(recordsCheckpoint, memoryKey{argString(args[0]), "strategy:" + argString(args[1]), argString(args[2])})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadEvent:
		records := tx.lookupRecords(recordsEvent, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Kind, r.Payload})
		}
		return rows, nil
	case opReadUnassignedCash:
		records := tx.lookupRecords(recordsAccount, memoryKey{argString(args[0]), "", ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Unassigned})
		}
		return rows, nil
	case opReadStrategyCash:
		records := tx.lookupRecords(recordsStrategy, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Cash})
		}
		return rows, nil
	case opReadAccountCash:
		records := tx.lookupRecords(recordsAccount, memoryKey{argString(args[0]), "", ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Cash})
		}
		return rows, nil
	case opReadPnLReclassification:
		records := tx.lookupRecords(recordsAccount, memoryKey{argString(args[0]), "", ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.PnlReclassification})
		}
		return rows, nil
	case opReadActualPosition:
		records := tx.lookupRecords(recordsPosition, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadAllocationFilled:
		records := tx.lookupRecords(recordsAllocation, memoryKey{argString(args[0]), argString(args[1]), argString(args[2])})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Filled})
		}
		return rows, nil
	case opReadAccountSnapshot:
		records := tx.lookupRecords(recordsAccount, memoryKey{argString(args[0]), "", ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Cash, r.Unassigned, r.PnlReclassification, r.Frozen, r.Checkpoint})
		}
		return rows, nil
	case opListStrategyCash:
		records := tx.selectRecords(recordsStrategy, func(r memoryRecord) bool { return r.Account == argString(args[0]) })
		sort.Slice(records, func(i, j int) bool { return records[i].Strategy < records[j].Strategy })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Strategy, r.Cash})
		}
		return rows, nil
	case opListActiveLots:
		records := tx.activeRecords(recordsLot, tx.memory.activeLots, func(r memoryRecord) bool { return r.Account == argString(args[0]) && r.Quantity != int64(0) })
		sort.Slice(records, func(i, j int) bool {
			if records[i].Strategy != records[j].Strategy {
				return records[i].Strategy < records[j].Strategy
			}
			return records[i].ID < records[j].ID
		})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opListActiveOrders:
		records := tx.activeRecords(recordsOrder, tx.memory.activeOrders, func(r memoryRecord) bool {
			return r.Account == argString(args[0]) && (r.State == "Prepared" || r.State == "Sending" || r.State == "Unknown" || r.State == "Acknowledged" || r.State == "Partial" || r.State == "CancelPending")
		})
		sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.ID})
		}
		return rows, nil
	case opListActualPositions:
		records := tx.selectRecords(recordsPosition, func(r memoryRecord) bool { return r.Account == argString(args[0]) })
		sort.Slice(records, func(i, j int) bool { return records[i].Instrument < records[j].Instrument })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opListExternalPositions:
		records := tx.selectRecords(recordsExternalPosition, func(r memoryRecord) bool { return r.Account == argString(args[0]) })
		sort.Slice(records, func(i, j int) bool { return records[i].Instrument < records[j].Instrument })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadMigration:
		records := tx.lookupRecords(recordsMigration, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.ID, r.SourceHash, r.SourceVersion, r.SchemaVersion, r.State})
		}
		return rows, nil
	case opReadLegacySource:
		records := tx.lookupRecords(recordsLegacySource, memoryKey{argString(args[0]), argString(args[1]), argString(args[2])})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadMigrationHash:
		records := tx.lookupRecords(recordsMigration, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.SourceHash, r.State})
		}
		return rows, nil
	case opCountMigrations:
		records := tx.selectRecords(recordsMigration, func(r memoryRecord) bool { return r.Account == argString(args[0]) })

		return [][]any{{int64(len(records))}}, nil
	case opReadMigrationState:
		records := tx.lookupRecords(recordsMigration, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.State, r.SourceHash})
		}
		return rows, nil
	case opReadAccountGenesis:
		records := tx.lookupRecords(recordsAccount, memoryKey{argString(args[0]), "", ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Cash, r.Unassigned, r.PnlReclassification, r.Checkpoint})
		}
		return rows, nil
	case opListCommittedEvents:
		account := tx.memory.records[recordsAccount][memoryKey{argString(args[0]), "", ""}]
		rows := make([][]any, 0)
		for checkpoint := argInt(args[1]) + 1; checkpoint <= account.Checkpoint && int64(len(rows)) < argInt(args[2]); checkpoint++ {
			id, present := tx.memory.eventCheckpoints[checkpoint]
			if !present {
				id = tx.historyIndex(recordsEvent, "checkpoint", checkpoint)
				present = id != ""
			}
			if tx.recordErr != nil {
				return nil, tx.recordErr
			}
			if !present {
				continue
			}
			r, _ := tx.memoryRecord(recordsEvent, memoryKey{argString(args[0]), id, ""})
			if tx.recordErr != nil {
				return nil, tx.recordErr
			}
			rows = append(rows, []any{r.ID, r.Kind, r.Checkpoint, r.Payload})
		}
		return rows, nil

	case opListEventPostings:
		records := tx.indexedRecords(recordsLedger, tx.memory.postingsByEvent, memoryKey{argString(args[0]), argString(args[1]), ""})
		sort.Slice(records, func(i, j int) bool { return records[i].PostingID < records[j].PostingID })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.PostingID, r.Kind, r.Strategy, r.Lot, r.Quantity, r.Cash, r.Fee, r.Realized, r.AtMs})
		}
		return rows, nil
	case opReadProjection:
		records := tx.lookupRecords(recordsCheckpoint, memoryKey{argString(args[0]), "projection:", argString(args[1])})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Checkpoint})
		}
		return rows, nil
	case opReadAccountCheckpoint:
		records := tx.lookupRecords(recordsAccount, memoryKey{argString(args[0]), "", ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Checkpoint})
		}
		return rows, nil
	case opListPlanOrders:
		records := tx.selectRecordsWhere(recordsOrder, " AND plan=?", []any{argString(args[1])}, func(r memoryRecord) bool { return r.Account == argString(args[0]) && r.PlanId == argString(args[1]) })
		sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.ID})
		}
		return rows, nil
	case opReadExternalPosition:
		records := tx.lookupRecords(recordsExternalPosition, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opListReconciliationPositions:
		records := tx.selectRecords(recordsPosition, func(r memoryRecord) bool { return r.Account == argString(args[0]) })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opCountPendingMigrations:
		records := tx.selectRecords(recordsMigration, func(r memoryRecord) bool { return r.Account == argString(args[0]) && r.State == "pending" })

		return [][]any{{int64(len(records))}}, nil
	case opCountExternalPositions:
		records := tx.selectRecords(recordsExternalPosition, func(r memoryRecord) bool { return r.Account == argString(args[0]) && r.Quantity != int64(0) })

		return [][]any{{int64(len(records))}}, nil
	case opListOrdersByState:
		records := tx.activeRecords(recordsOrder, tx.memory.activeOrders, func(r memoryRecord) bool { return r.Account == argString(args[0]) && r.State == argString(args[1]) })

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.ID})
		}
		return rows, nil
	case opReadIntentDefinition:
		records := tx.lookupRecords(recordsVirtualIntent, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadOrderDefinition:
		records := tx.lookupRecords(recordsOrder, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload})
		}
		return rows, nil
	case opReadOrder:
		records := tx.lookupRecords(recordsOrder, memoryKey{argString(args[0]), argString(args[1]), ""})

		rows := make([][]any, 0, len(records))
		for _, r := range records {
			rows = append(rows, []any{r.Payload, r.ClientId, r.ExchangeId, r.State, r.Filled, r.Fee, r.Cost, r.Attempt, r.Generation})
		}
		return rows, nil
	case opReadLatestSequence, opReadPlanSequence:
		return [][]any{{tx.memory.latestSequence}}, nil

	case opReadLatestPlan:
		r, present := tx.memoryRecord(recordsPlan, memoryKey{argString(args[0]), tx.memory.latestPlanID, ""})
		if !present {
			return nil, nil
		}
		return [][]any{{r.Payload}}, nil

	case opFindVenueOrder:
		rows := [][]any{}
		for _, r := range tx.selectRecordsWhere(recordsOrder, " AND ((?<>'' AND client=?) OR (?<>'' AND exchange=?))", args[1:], func(r memoryRecord) bool {
			return r.Account == argString(args[0]) && (argString(args[1]) != "" && r.ClientId == argString(args[2]) || argString(args[3]) != "" && r.ExchangeId == argString(args[4]))
		}) {
			if r.Account == argString(args[0]) && (argString(args[1]) != "" && r.ClientId == argString(args[2]) || argString(args[3]) != "" && r.ExchangeId == argString(args[4])) {
				rows = append(rows, []any{r.ID})
			}
		}
		return rows, nil
	case opReadIntentFilled, opReadIntentReserved, opReadIntentConsumed, opReadAllocatedSteps:
		var total int64
		for _, r := range tx.indexedRecords(recordsAllocation, tx.memory.allocationsByIntent, memoryKey{argString(args[0]), argString(args[1]), ""}) {
			if r.Account != argString(args[0]) || r.IntentId != argString(args[1]) {
				continue
			}
			order, ok := tx.memoryRecord(recordsOrder, memoryKey{r.Account, r.OrderId, ""})
			if !ok {
				return nil, errors.New("execution: allocation missing order")
			}
			var addition int64
			switch operation {
			case opReadIntentFilled:
				addition = r.Filled
			case opReadIntentReserved:
				if !terminalOrder(order.State) {
					addition = r.Steps - r.Filled
				}
			case opReadIntentConsumed:
				if order.State == "Canceled" || order.State == "Rejected" {
					addition = r.Filled
				} else {
					addition = r.Steps
				}
			case opReadAllocatedSteps:
				addition = r.Steps
			}
			var err error
			total, err = checkedSteps(total, addition)
			if err != nil {
				return nil, err
			}
		}
		if operation == opReadIntentFilled || operation == opReadIntentConsumed {
			for _, r := range tx.indexedRecords(recordsInternalAllocation, tx.memory.internalByIntent, memoryKey{argString(args[0]), argString(args[1]), ""}) {
				if r.Account == argString(args[0]) && r.IntentId == argString(args[1]) {
					var err error
					total, err = checkedSteps(total, r.Steps)
					if err != nil {
						return nil, err
					}
				}
			}
		}
		return [][]any{{total}}, nil
	case opReadPlanIntentRuntime, opReadPlanIntentDefinition:
		planIndex := 3
		if operation == opReadPlanIntentDefinition {
			planIndex = 2
		}
		member := memoryKey{argString(args[0]), argString(args[planIndex]), argString(args[1])}
		if _, ok := tx.memoryRecord(recordsPlanIntent, member); !ok {
			return nil, nil
		}
		r, ok := tx.memoryRecord(recordsVirtualIntent, memoryKey{argString(args[0]), argString(args[1]), ""})
		if !ok {
			return nil, nil
		}
		body := r.Payload
		if operation == opReadPlanIntentRuntime {
			body = r.runtimeBody()
		}
		return [][]any{{body}}, nil
	case opCountMigrationDestination:
		var total int64
		for _, kind := range []recordKind{recordsPlan, recordsLot, recordsPosition, recordsStrategy, recordsEvent, recordsCheckpoint, recordsExternalPosition} {
			for _, r := range tx.selectRecords(kind, func(r memoryRecord) bool { return r.Account == argString(args[0]) }) {
				if r.Account == argString(args[0]) && !(kind == recordsCheckpoint && (r.Kind != "strategy" || r.Strategy == "account-risk" && r.Name == "policy-v1")) {
					total++
				}
			}
		}
		return [][]any{{total}}, nil
	default:
		return nil, errors.New("execution: unsupported memory read")
	}
}

func (tx *storeTxn) memoryWrite(operation storageOperation, args []any) error {
	switch operation {
	case opPutStrategyCheckpoint:
		r := memoryRecord{Account: argString(args[0]), Kind: "strategy", Strategy: argString(args[1]), Name: argString(args[2]), Payload: argString(args[3])}
		return tx.putRecord(recordsCheckpoint, r, replaceRecord, func(old *memoryRecord) { old.Payload = r.Payload })
	case opUpdateOrderAttempt:
		return tx.updateRecord(recordsOrder, memoryKey{argString(args[2]), argString(args[3]), ""}, func(r *memoryRecord) { r.Attempt = argInt(args[0]); r.Generation = argString(args[1]) })

	case opUpdateExchangeID:
		return tx.updateRecord(recordsOrder, memoryKey{argString(args[1]), argString(args[2]), ""}, func(r *memoryRecord) { r.ExchangeId = argString(args[0]) })

	case opUpdateIntentRuntime:
		return tx.updateRecord(recordsVirtualIntent, memoryKey{argString(args[1]), argString(args[2]), ""}, func(r *memoryRecord) { r.RuntimePayload = argString(args[0]) })
	case opInsertEvent:
		r := memoryRecord{Account: argString(args[0]), ID: argString(args[1]), Kind: argString(args[2]), Payload: argString(args[3])}
		if r.Kind == "OrderAttempt" {
			var attempt OrderAttempt
			if err := json.Unmarshal([]byte(r.Payload), &attempt); err != nil {
				return err
			}
			r.OrderId = attempt.OrderID
		}
		return tx.putRecord(recordsEvent, r, insertRecord, nil)
	case opInsertPosting:
		r := memoryRecord{Account: argString(args[0]), EventId: argString(args[1]), Kind: argString(args[2]), Strategy: argString(args[3]), Lot: argString(args[4]), Quantity: argInt(args[5]), Cash: argString(args[6]), Fee: argString(args[7]), Realized: argString(args[8]), AtMs: argInt(args[9])}
		return tx.putRecord(recordsLedger, r, insertRecord, nil)
	case opUpdateUnassignedCash:
		return tx.updateRecord(recordsAccount, memoryKey{argString(args[1]), "", ""}, func(r *memoryRecord) { r.Unassigned = argString(args[0]) })
	case opPutStrategyCash:
		r := memoryRecord{Account: argString(args[0]), Strategy: argString(args[1]), Cash: argString(args[2])}
		return tx.putRecord(recordsStrategy, r, replaceRecord, func(old *memoryRecord) { old.Cash = r.Cash })
	case opEnsureStrategy:
		r := memoryRecord{Account: argString(args[0]), Strategy: argString(args[1]), Cash: argString(args[2])}
		return tx.putRecord(recordsStrategy, r, ignoreRecord, nil)
	case opUpdateStrategyTotals:
		return tx.updateRecord(recordsStrategy, memoryKey{argString(args[2]), argString(args[3]), ""}, func(r *memoryRecord) { r.Fees = argString(args[0]); r.Funding = argString(args[1]) })
	case opUpdateAccountCash:
		return tx.updateRecord(recordsAccount, memoryKey{argString(args[1]), "", ""}, func(r *memoryRecord) { r.Cash = argString(args[0]); r.Checkpoint = r.Checkpoint + 1 })
	case opUpdateAccountFrozen:
		return tx.updateRecord(recordsAccount, memoryKey{argString(args[1]), "", ""}, func(r *memoryRecord) { r.Frozen = argBool(args[0]) })
	case opUpdatePnLReclassification:
		return tx.updateRecord(recordsAccount, memoryKey{argString(args[1]), "", ""}, func(r *memoryRecord) { r.PnlReclassification = argString(args[0]) })
	case opPutLot:
		r := memoryRecord{Account: argString(args[0]), Strategy: argString(args[1]), ID: argString(args[2]), Quantity: argInt(args[3]), Payload: argString(args[4])}
		return tx.putRecord(recordsLot, r, replaceRecord, func(old *memoryRecord) { old.Quantity = r.Quantity; old.Payload = r.Payload })
	case opPutActualPosition:
		r := memoryRecord{Account: argString(args[0]), Instrument: argString(args[1]), Payload: argString(args[2])}
		return tx.putRecord(recordsPosition, r, replaceRecord, func(old *memoryRecord) { old.Payload = r.Payload })
	case opUpdateReportMode:
		return tx.updateRecord(recordsOrder, memoryKey{argString(args[1]), argString(args[2]), ""}, func(r *memoryRecord) { r.ReportMode = argString(args[0]) })
	case opIncrementAllocationFilled:
		return tx.updateRecord(recordsAllocation, memoryKey{argString(args[1]), argString(args[2]), argString(args[3])}, func(r *memoryRecord) { r.Filled = r.Filled + argInt(args[0]) })
	case opUpdateFillHighwater:
		return tx.updateRecord(recordsOrder, memoryKey{argString(args[4]), argString(args[5]), ""}, func(r *memoryRecord) {
			r.Filled = argInt(args[0])
			r.Fee = argString(args[1])
			r.Cost = argString(args[2])
			r.ReportMode = argString(args[3])
		})
	case opUpdateOrderFee:
		return tx.updateRecord(recordsOrder, memoryKey{argString(args[1]), argString(args[2]), ""}, func(r *memoryRecord) { r.Fee = argString(args[0]) })
	case opInsertMigration:
		r := memoryRecord{Account: argString(args[0]), ID: argString(args[1]), SourceHash: argString(args[2]), SourceVersion: argString(args[3]), SchemaVersion: argInt(args[4]), State: "pending", Payload: argString(args[5])}
		return tx.putRecord(recordsMigration, r, insertRecord, nil)
	case opFreezeAccount:
		return tx.updateRecord(recordsAccount, memoryKey{argString(args[0]), "", ""}, func(r *memoryRecord) { r.Frozen = true })
	case opInsertStrategy:
		r := memoryRecord{Account: argString(args[0]), Strategy: argString(args[1]), Cash: argString(args[2])}
		return tx.putRecord(recordsStrategy, r, insertRecord, nil)
	case opInsertLot:
		r := memoryRecord{Account: argString(args[0]), Strategy: argString(args[1]), ID: argString(args[2]), Quantity: argInt(args[3]), Payload: argString(args[4])}
		return tx.putRecord(recordsLot, r, insertRecord, nil)
	case opInsertActualPosition:
		r := memoryRecord{Account: argString(args[0]), Instrument: argString(args[1]), Payload: argString(args[2])}
		return tx.putRecord(recordsPosition, r, insertRecord, nil)
	case opInsertLegacySource:
		r := memoryRecord{Account: argString(args[0]), MigrationId: argString(args[1]), Strategy: argString(args[2]), Lot: argString(args[3]), Payload: argString(args[4])}
		return tx.putRecord(recordsLegacySource, r, insertRecord, nil)
	case opUpdateAccountGenesis:
		return tx.updateRecord(recordsAccount, memoryKey{argString(args[3]), "", ""}, func(r *memoryRecord) {
			r.Cash = argString(args[0])
			r.Unassigned = argString(args[1])
			r.PnlReclassification = argString(args[2])
		})
	case opFinishMigration:
		return tx.updateRecord(recordsMigration, memoryKey{argString(args[0]), argString(args[1]), ""}, func(r *memoryRecord) { r.State = "ready" })
	case opInsertImportedOrder:
		r := memoryRecord{Account: argString(args[0]), ID: argString(args[1]), PlanId: argString(args[2]), Payload: argString(args[3]), ClientId: argString(args[4]), ExchangeId: argString(args[5]), State: argString(args[6]), Filled: argInt(args[7]), Fee: argString(args[8]), Cost: argString(args[9]), ReportMode: "cumulative", Attempt: argInt(args[10]), Generation: argString(args[11])}
		return tx.putRecord(recordsOrder, r, insertRecord, nil)
	case opInsertImportedAllocation:
		r := memoryRecord{Account: argString(args[0]), OrderId: argString(args[1]), ID: argString(args[2]), IntentId: argString(args[3]), Strategy: argString(args[4]), Lot: argString(args[5]), Steps: argInt(args[6]), Filled: argInt(args[7])}
		return tx.putRecord(recordsAllocation, r, insertRecord, nil)
	case opPutProjection:
		r := memoryRecord{Account: argString(args[0]), Kind: "projection", Name: argString(args[1]), Checkpoint: argInt(args[2])}
		return tx.putRecord(recordsCheckpoint, r, replaceRecord, func(old *memoryRecord) { old.Checkpoint = r.Checkpoint })
	case opInsertInternalAllocation:
		r := memoryRecord{Account: argString(args[0]), EventId: argString(args[1]), IntentId: argString(args[2]), Steps: argInt(args[3])}
		return tx.putRecord(recordsInternalAllocation, r, insertRecord, nil)
	case opPutExternalPosition:
		r := memoryRecord{Account: argString(args[0]), Instrument: argString(args[1]), Quantity: argInt(args[2]), Payload: argString(args[3])}
		return tx.putRecord(recordsExternalPosition, r, replaceRecord, func(old *memoryRecord) { old.Quantity = r.Quantity; old.Payload = r.Payload })
	case opInsertPlan:
		r := memoryRecord{Account: argString(args[0]), ID: argString(args[1]), Sequence: argInt(args[2]), Payload: argString(args[3])}
		return tx.putRecord(recordsPlan, r, insertRecord, nil)
	case opEnsureIntent:
		r := memoryRecord{Account: argString(args[0]), ID: argString(args[1]), PlanId: argString(args[2]), Payload: argString(args[3]), RuntimePayload: argString(args[4])}
		return tx.putRecord(recordsVirtualIntent, r, ignoreRecord, nil)
	case opInsertPlanMembership:
		r := memoryRecord{Account: argString(args[0]), PlanId: argString(args[1]), IntentId: argString(args[2])}
		return tx.putRecord(recordsPlanIntent, r, insertRecord, nil)
	case opInsertOrder:
		r := memoryRecord{Account: argString(args[0]), ID: argString(args[1]), PlanId: argString(args[2]), Payload: argString(args[3]), ClientId: argString(args[4]), State: argString(args[5])}
		return tx.putRecord(recordsOrder, r, insertRecord, nil)
	case opInsertAllocation:
		r := memoryRecord{Account: argString(args[0]), OrderId: argString(args[1]), ID: argString(args[2]), IntentId: argString(args[3]), Strategy: argString(args[4]), Lot: argString(args[5]), Steps: argInt(args[6])}
		return tx.putRecord(recordsAllocation, r, insertRecord, nil)
	case opUpdateOrderState:
		return tx.updateRecord(recordsOrder, memoryKey{argString(args[1]), argString(args[2]), ""}, func(r *memoryRecord) { r.State = argString(args[0]) })

	case opCheckpointEvent:
		account := tx.memory.records[recordsAccount][memoryKey{argString(args[0]), "", ""}]
		return tx.updateRecord(recordsEvent, memoryKey{argString(args[1]), argString(args[2]), ""}, func(r *memoryRecord) { r.Checkpoint = account.Checkpoint })
	default:
		return errors.New("execution: unsupported memory write")
	}
}
