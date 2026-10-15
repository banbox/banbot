package execution

import "context"

// recoveryOrdersAfter includes terminal and cold orders with a durable send or
// imported venue identity. Never-sent local orders have no venue history.
func (s *Store) recoveryOrdersAfter(ctx context.Context, after string) ([]string, error) {
	var ids []string
	err := s.commit(ctx, func(tx *storeTxn) error {
		rows, err := tx.Query(opListRecoveryOrders, s.accountID, after, 64)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return rows.Err()
	})
	return ids, err
}
