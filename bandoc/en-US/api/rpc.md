# rpc Package

Notifications and remote commands bind to a per-Runtime rpc.Session, preventing channels, order managers, wallets and language settings from crossing task boundaries.

## Session

NewSession(snapshot *config.Snapshot, accounts map[string]*config.AccountConfig) *Session derives a session from the snapshot, execution accounts and directories. Bind Core, Clock, Orders and Wallets to the same task; messages and remote commands use these instance dependencies.

Session.Start() constructs configured channels and starts consumers. Stop/Join participate in Runtime shutdown, close admission and wait for admitted work. SMTP senders use session mail configuration rather than another task's global settings.

Telegram, WeCom, webhook and email configuration remain available. Remote trading commands use the session's order/wallet interfaces, not a CurrentRuntime lookup. Compatibility notification helpers do not prove multi-task isolation.

Local Session/lifecycle tests do not certify real email/platform delivery. See [runtime](runtime.md).
