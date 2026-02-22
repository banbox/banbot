# web Package

The web package provides functionality for Web server and API interfaces.

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