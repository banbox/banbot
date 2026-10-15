package execution

import (
	"context"
	"errors"
)

// StrategyCheckpointsAfter reads a bounded lexical page within one strategy
// and exact prefix. Callers requiring a fixed snapshot hold account ownership
// while choosing their output boundary; each page reads only committed state.
func (s *Store) StrategyCheckpointsAfter(ctx context.Context, strategy StrategyID, prefix, afterName string, limit int) ([]StrategyCheckpoint, error) {
	if !canonicalID(string(strategy)) || !canonicalID(prefix) || limit <= 0 {
		return nil, errors.New("execution: strategy, prefix and positive checkpoint page limit required")
	}
	var result []StrategyCheckpoint
	err := s.commit(ctx, func(tx *storeTxn) error {
		rows, err := tx.Query(opListStrategyCheckpointPage, s.accountID, string(strategy), prefix, afterName, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			record := StrategyCheckpoint{Strategy: strategy}
			var body string
			if err := rows.Scan(&record.Name, &body); err != nil {
				return err
			}
			record.Payload = []byte(body)
			result = append(result, record)
		}
		return rows.Err()
	})
	return result, err
}
