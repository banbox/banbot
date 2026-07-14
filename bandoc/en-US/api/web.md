# web Package

The web package provides functionality for Web server and API interfaces.

## API Routes

After the real-time API starts, it provides the following route groups:

- `/api/kline`: symbols, historical K-lines, indicator calculations, CSV, and read-only `data_sources` and `series` queries for registered sources.
- `/api/ws/ohlcv`: OHLCV websocket.
- `/api/bot`: balances, orders, strategy jobs, bot configuration, and trading controls; authentication required.
- `/api/login`, `/api/ping`, `/api/strat_call`: public authentication and strategy-call interfaces.

`GET /api/kline/series` queries built-in K-lines or registered custom time-series data using `source`, `sid`, `timeframe`, `start`, `end`, `limit`, and optional `fields`. The endpoint is read-only and returns at most 1,000 rows per request.

### RunDev
Run the Web UI robot control panel. This method is used to start the Web interface in the development environment for robot management and monitoring.

Parameters:
- `args []string` - Command line argument list

Returns:
- `error` - Returns corresponding error information if an error occurs during startup; returns nil if startup is successful

### StartApi
Start the Web monitoring panel for real-time trading. This method is used to start a Web server that provides monitoring and management functionality for real-time trading data.

Returns:
- `*errs.Error` - Returns a custom error type if an error occurs during startup; returns nil if startup is successful
