package ormu

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg/errs"
)

func Conn() (*Queries, *orm.TrackedDB, *errs.Error) {
	db, err := orm.DbLite(orm.DbUI, core.DevDbPath, true, 5000)
	if err != nil {
		return nil, nil, err
	}
	return New(db), db, nil
}

const (
	BtStatusInit = iota + 1
	BtStatusRunning
	BtStatusDone
	BtStatusFail
)
