package core

import "github.com/banbox/banexg/errs"

const (
	ErrRunTime        = -100
	ErrTimeout        = -101
	ErrCacheErr       = -102
	ErrMarshalFail    = -103
	ErrCompressFail   = -104
	ErrDeCompressFail = -105
	ErrDecryptFail    = -106
	ErrEncryptFail    = -107

	// invalid parameter

	ErrBadConfig     = -110
	ErrInvalidPath   = -111
	ErrInvalidTF     = -112
	ErrInvalidSymbol = -113
	ErrInvalidBars   = -114
	ErrInvalidAddr   = -115
	ErrInvalidCost   = -116

	// database

	ErrDbConnFail        = -120
	ErrDbReadFail        = -121
	ErrDbExecFail        = -122
	ErrDbUniqueViolation = -123

	// trade

	ErrLiquidation  = -130
	ErrLowFunds     = -131
	ErrLowSrcAmount = -132
	ErrExgNotInit   = -133

	// network

	ErrNetWriteFail = -140
	ErrNetReadFail  = -141
	ErrNetUnknown   = -142
	ErrNetTimeout   = -143
	ErrNetTemporary = -144
	ErrNetConnect   = -145

	// IO

	ErrIOReadFail  = -150
	ErrIOWriteFail = -151
	ErrEOF         = -152

	// other

	ErrAuthFail      = -200
	ErrRemoteError   = -201
	ErrOutOfResource = -202
)

func init() {
	errs.UpdateErrNames(map[int]string{
		ErrBadConfig:         "BadConfig",
		ErrInvalidPath:       "InvalidPath",
		ErrIOReadFail:        "IOReadFail",
		ErrIOWriteFail:       "IOWriteFail",
		ErrDbConnFail:        "DbConnFail",
		ErrDbReadFail:        "DbReadFail",
		ErrDbExecFail:        "DbExecFail",
		ErrDbUniqueViolation: "DbUniqueViolation",
		ErrLiquidation:       "Liquidation",
		ErrLowFunds:          "LowFunds",
		ErrLowSrcAmount:      "LowSrcAmount",
		ErrInvalidCost:       "InvalidCost",
		ErrExgNotInit:        "ExgNotInit",
		ErrCacheErr:          "CacheErr",
		ErrInvalidTF:         "InvalidTF",
		ErrInvalidSymbol:     "InvalidSymbol",
		ErrInvalidBars:       "InvalidBars",
		ErrInvalidAddr:       "InvalidAddr",
		ErrRunTime:           "RunTime",
		ErrMarshalFail:       "MarshalFail",
		ErrCompressFail:      "CompressFail",
		ErrDeCompressFail:    "DeCompressFail",
		ErrDecryptFail:       "DecryptFail",
		ErrEncryptFail:       "EncryptFail",
		ErrTimeout:           "Timeout",
		ErrEOF:               "EOF",
		ErrNetWriteFail:      "NetWriteFail",
		ErrNetReadFail:       "NetReadFail",
		ErrNetUnknown:        "NetUnknown",
		ErrNetTimeout:        "NetTimeout",
		ErrNetTemporary:      "NetTemporary",
		ErrNetConnect:        "NetConnect",
		ErrAuthFail:          "AuthFail",
		ErrRemoteError:       "RemoteError",
		ErrOutOfResource:     "OutOfResource",
	})
}
