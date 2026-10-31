package entry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/banbox/banbot/execution"
	"github.com/shopspring/decimal"
)

const hubHostManifestName = "host-declaration-v1.json"

// The declaration deliberately has no credential field. A reconnecting robot
// supplies credentials; the host retains the original ownership/universe even
// when that initial robot is offline.
type hubHostDeclaration struct {
	Version                            int
	Account                            execution.AccountKey
	Env                                string
	Exchange                           string
	Instruments                        []execution.BanexgInstrument
	Capital                            map[execution.StrategyID]decimal.Decimal
	Risk                               execution.PortfolioRisk
	Takeover                           execution.StrategyID
	PaperCash, PaperFee, PaperSlippage decimal.Decimal
}

func hubAccountDirectory(root string, key execution.AccountKey) string {
	body, _ := json.Marshal(key)
	return filepath.Join(root, fmt.Sprintf("%x", sha256.Sum256(body)))
}

func (d hubHostDeclaration) config() HubAccountConfig {
	return HubAccountConfig{Exchange: d.Exchange, Instruments: d.Instruments, Capital: d.Capital, Risk: d.Risk, Takeover: d.Takeover, PaperCash: d.PaperCash, PaperFee: d.PaperFee, PaperSlippage: d.PaperSlippage}
}

func readHubHostDeclaration(root string, request execution.HubAccountRegistration) (HubAccountConfig, error) {
	path := filepath.Join(hubAccountDirectory(root, request.Account), hubHostManifestName)
	info, err := os.Lstat(path)
	if err != nil {
		return HubAccountConfig{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return HubAccountConfig{}, errors.New("hub: host declaration must be a private regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return HubAccountConfig{}, err
	}
	var declaration hubHostDeclaration
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&declaration); err != nil {
		return HubAccountConfig{}, fmt.Errorf("hub: invalid host declaration: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return HubAccountConfig{}, errors.New("hub: trailing host declaration data")
	}
	if declaration.Version != 1 || declaration.Account != request.Account || declaration.Env != request.Env {
		return HubAccountConfig{}, errors.New("hub: host declaration identity mismatch")
	}
	return declaration.config(), nil
}

// Validate the immutable account baseline before creating a venue or allocating
// a contributor. Strategy additions remain an atomic ledger admission afterward.
func validateHubHostRegistration(initial, incoming HubAccountConfig) error {
	for _, unit := range incoming.Instruments {
		if !slices.ContainsFunc(initial.Instruments, func(host execution.BanexgInstrument) bool {
			return host.Symbol == unit.Symbol && sameFactorInstrument(host.Instrument, unit.Instrument)
		}) {
			return errors.New("hub: additional robot requires host instrument expansion")
		}
	}
	baseline := func(c HubAccountConfig) ([]byte, error) {
		c.Credential = nil
		c.Capital = nil
		c.Instruments = nil
		c.Risk.StrategyGrossLimits = nil
		return json.Marshal(c)
	}
	want, err := baseline(initial)
	if err != nil {
		return err
	}
	got, err := baseline(incoming)
	if err != nil || string(want) != string(got) {
		return errors.New("hub: immutable account registration baseline mismatch")
	}
	for id, capital := range incoming.Capital {
		if original, exists := initial.Capital[id]; exists && (!original.Equal(capital) || !initial.Risk.StrategyGrossLimits[id].Equal(incoming.Risk.StrategyGrossLimits[id])) {
			return errors.New("hub: immutable bootstrap strategy registration mismatch")
		}
	}
	return nil
}

// loadOrCreateHubHostConfig commits the initial public declaration before any
// venue/account creation. Link publishes a fsynced private temp file without
// replacing a declaration created by a competing host.
func loadOrCreateHubHostConfig(ctx context.Context, root string, request execution.HubAccountRegistration, incoming HubAccountConfig) (HubAccountConfig, error) {
	if err := ctx.Err(); err != nil {
		return HubAccountConfig{}, err
	}
	directory := hubAccountDirectory(root, request.Account)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return HubAccountConfig{}, err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return HubAccountConfig{}, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return HubAccountConfig{}, errors.New("hub: account declaration directory must be private")
	}
	initial, err := readHubHostDeclaration(root, request)
	if errors.Is(err, os.ErrNotExist) {
		// Never promote the first reconnecting subset into the initial owner of an
		// existing ledger. Its omitted strategies may still own active protection.
		ledger, statErr := os.Lstat(filepath.Join(directory, "ledger.db"))
		if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
			return HubAccountConfig{}, statErr
		}
		if statErr == nil && (!ledger.Mode().IsRegular() || ledger.Size() != 0) {
			return HubAccountConfig{}, errors.New("hub: existing ledger has no initial host declaration; refusing inferred ownership")
		}
		declaration := hubHostDeclaration{Version: 1, Account: request.Account, Env: request.Env, Exchange: incoming.Exchange, Instruments: incoming.Instruments, Capital: incoming.Capital, Risk: incoming.Risk, Takeover: incoming.Takeover, PaperCash: incoming.PaperCash, PaperFee: incoming.PaperFee, PaperSlippage: incoming.PaperSlippage}
		body, marshalErr := json.Marshal(declaration)
		if marshalErr != nil {
			return HubAccountConfig{}, marshalErr
		}
		file, createErr := os.CreateTemp(directory, ".host-declaration-*")
		if createErr != nil {
			return HubAccountConfig{}, createErr
		}
		temporary := file.Name()
		defer os.Remove(temporary)
		if _, err = file.Write(body); err == nil {
			err = file.Sync()
		}
		err = errors.Join(err, file.Close())
		if err != nil {
			return HubAccountConfig{}, err
		}
		if err := ctx.Err(); err != nil {
			return HubAccountConfig{}, err
		}
		err = os.Link(temporary, filepath.Join(directory, hubHostManifestName))
		if err != nil && !errors.Is(err, os.ErrExist) {
			return HubAccountConfig{}, err
		}
		initial, err = readHubHostDeclaration(root, request)
	}
	if err != nil {
		return HubAccountConfig{}, err
	}
	// Run normal validation on the full declaration, using only this request's
	// ephemeral credentials. Corrupt/partial declarations cannot weaken risk.
	initial.Credential = incoming.Credential
	restored := request
	restored.Configuration, err = json.Marshal(initial)
	if err != nil {
		return HubAccountConfig{}, err
	}
	initial, err = decodeHubAccountConfig(restored)
	if err != nil {
		return HubAccountConfig{}, err
	}
	if err := validateHubHostRegistration(initial, incoming); err != nil {
		return HubAccountConfig{}, err
	}
	// Include the parent entry for a newly created account directory. Also
	// retry these durability barriers when an earlier sync failed after link.
	for _, path := range []string{directory, filepath.Dir(directory)} {
		dir, err := os.Open(path)
		if err != nil {
			return HubAccountConfig{}, err
		}
		err = errors.Join(dir.Sync(), dir.Close())
		if err != nil {
			return HubAccountConfig{}, err
		}
	}
	return initial, nil
}
