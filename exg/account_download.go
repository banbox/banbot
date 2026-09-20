package exg

import (
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
)

// AccountDownloadCapability keeps asynchronous export request details in the
// exchange adapter. The source name is normalized by banbot and interpreted by
// the adapter that owns the endpoint.
type AccountDownloadCapability interface {
	StartAccountDownload(source, account string, startMS, endMS, timestamp int64) (*banexg.HttpRes, *errs.Error)
	GetAccountDownload(source, account, downloadID string, timestamp int64) (*banexg.HttpRes, *errs.Error)
}

// StartAccountDownload starts an exchange-owned asynchronous account export.
// WebUI callers only select the normalized export source.
func StartAccountDownload(exchange banexg.BanExchange, source, account string, startMS, endMS, timestamp int64) (*banexg.HttpRes, *errs.Error) {
	if exchange == nil {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "exchange is required for account download")
	}
	capability := GetAccountDownloadCapability(exchange)
	if capability == nil {
		return nil, accountDownloadUnsupported()
	}
	return callAccountDownload(func() (*banexg.HttpRes, *errs.Error) {
		return capability.StartAccountDownload(source, account, startMS, endMS, timestamp)
	})
}

// GetAccountDownload fetches a previously started exchange-owned account
// export. It mirrors StartAccountDownload's normalized source API.
func GetAccountDownload(exchange banexg.BanExchange, source, account, downloadID string, timestamp int64) (*banexg.HttpRes, *errs.Error) {
	if exchange == nil {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "exchange is required for account download")
	}
	capability := GetAccountDownloadCapability(exchange)
	if capability == nil {
		return nil, accountDownloadUnsupported()
	}
	return callAccountDownload(func() (*banexg.HttpRes, *errs.Error) {
		return capability.GetAccountDownload(source, account, downloadID, timestamp)
	})
}

// GetAccountDownloadCapability unwraps the local BotExchange boundary without
// requiring callers to know how the runtime wraps a banexg adapter.
func GetAccountDownloadCapability(exchange banexg.BanExchange) AccountDownloadCapability {
	capability, _ := getExchangeCapability[AccountDownloadCapability](exchange)
	return capability
}

func callAccountDownload(call func() (*banexg.HttpRes, *errs.Error)) (res *banexg.HttpRes, err *errs.Error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			res = nil
			err = errs.NewMsg(core.ErrRunTime, "account download capability panicked: %v", recovered)
		}
	}()
	return call()
}

func accountDownloadUnsupported() *errs.Error {
	return errs.NewMsg(errs.CodeNotSupport, "exchange adapter does not support account download")
}

// ArchiveURLCapability keeps archive endpoint construction in the adapter that
// owns the archive format. It is separate from OHLCVArchiveFetcher because the
// legacy data loader still needs a URL before it starts its file worker.
type ArchiveURLCapability interface {
	BuildArchiveURL(market, dataType, rawSymbol, date string) (string, *errs.Error)
}

// GetArchiveURLCapability unwraps the local BotExchange boundary.
func GetArchiveURLCapability(exchange banexg.BanExchange) ArchiveURLCapability {
	capability, _ := getExchangeCapability[ArchiveURLCapability](exchange)
	return capability
}

// BuildArchiveURL preserves the old ID-based facade while delegating URL
// construction to the matching adapter capability.
func BuildArchiveURL(exchangeID, market, dataType, rawSymbol, date string) (string, *errs.Error) {
	if exchangeID == "" {
		return "", errs.NewMsg(errs.CodeParamInvalid, "exchange is required for archive data")
	}
	exchange, err := GetWith(exchangeID, "", "")
	if err != nil {
		return "", err
	}
	return BuildArchiveURLForExchange(exchange, market, dataType, rawSymbol, date)
}

// BuildArchiveURLForExchange is the checked adapter-owned form used by code
// that already has the exchange instance and should avoid an adapter lookup.
func BuildArchiveURLForExchange(exchange banexg.BanExchange, market, dataType, rawSymbol, date string) (string, *errs.Error) {
	if exchange == nil {
		return "", errs.NewMsg(errs.CodeParamInvalid, "exchange is required for archive data")
	}
	if rawSymbol == "" || dataType == "" || date == "" {
		return "", errs.NewMsg(errs.CodeParamInvalid,
			"archive symbol requires raw symbol, data type, and date")
	}
	capability := GetArchiveURLCapability(exchange)
	if capability == nil {
		return "", errs.NewMsg(errs.CodeNotSupport, "exchange adapter does not support archive data")
	}
	return callArchiveURLCapability(capability, market, dataType, rawSymbol, date)
}

func callArchiveURLCapability(capability ArchiveURLCapability, market, dataType, rawSymbol, date string) (url string, err *errs.Error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			url = ""
			err = errs.NewMsg(core.ErrRunTime, "archive URL capability panicked: %v", recovered)
		}
	}()
	return capability.BuildArchiveURL(market, dataType, rawSymbol, date)
}
