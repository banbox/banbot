package orm

import (
	"context"
	"database/sql"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func BanPubConn(write bool) (*TrackedDB, *errs.Error) {
	return DbLite(DbPub, DbPub, write, 10_000)
}

func WithBanPubTx(ctx context.Context, fn func(tx *sql.Tx) error) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := BanPubConn(true)
	if err != nil {
		return err
	}
	defer db.Close()

	// 使用 IMMEDIATE 模式，在事务开始时就获取写锁，避免后续锁升级冲突
	tx, err_ := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err_ != nil {
		return errs.New(core.ErrDbConnFail, err_)
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()
	if err_ := fn(tx); err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	if err_ := tx.Commit(); err_ != nil {
		return errs.New(core.ErrDbExecFail, err_)
	}
	commit = true
	return nil
}
