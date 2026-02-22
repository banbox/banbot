# rpc Package

The rpc package provides remote procedure call functionality, mainly used for message notifications and exception reporting.

## Main Structures

### IWebHook
`IWebHook` is an interface that defines the basic behavior of a WebHook notification channel:
- `GetName() string` - Get WebHook name
- `IsDisable() bool` - Check if WebHook is disabled
- `SetDisable(val bool)` - Set WebHook disable status
- `CleanUp()` - Clean up WebHook resources
- `SendMsg(msgType string, account string, payload map[string]string) bool` - Send message
- `ConsumeForever()` - Continuously consume message queue

### WebHook
`WebHook` is the base WebHook implementation structure, containing the following main fields:
- `name string` - WebHook name
- `Config map[string]interface{}` - WebHook configuration
- `MsgTypes map[string]bool` - Supported message types mapping
- `Accounts map[string]bool` - Supported accounts mapping
- `Queue chan map[string]string` - Message queue
- `RetryNum int` - Number of retries
- `RetryDelay int` - Retry interval (seconds)
- `Disable bool` - Whether disabled
- `ChlType string` - Channel type

### WeWork
`WeWork` is the WeCom (WeChat Work) WebHook implementation, inheriting from WebHook, with the following specific fields:
- `cred *WeCred` - WeCom authentication information
- `agentId string` - Application ID
- `toUser string` - Message recipients (users)
- `toParty string` - Message recipients (departments)
- `toTag string` - Message recipients (tags)

## Main Features

### InitRPC
Initialize RPC service, primarily used to initialize WebHook notification channels.


Returns:
- `*errs.Error` - Error during initialization, returns nil if successful

### SendMsg
Send messages to configured notification channels.

Parameters:
- `msg map[string]interface{}` - Message content, including the following fields:
  - `type` - Message type
  - `account` - Account identifier (optional)
  - `status` - Message status content
  - Other fields are rendered according to webhook configuration template

### CleanUp
Clean up RPC-related resources, close all notification channels.


### NewExcNotify
Create a new exception notification handler for capturing and sending error logs.


Returns:
- `*ExcNotify` - Exception notification handler instance

### TrySendExc
Try to send exception information, will merge identical exceptions.

Parameters:
- `cacheKey string` - Exception cache key, typically the call location
- `content string` - Exception content

### NewWebHook
Create a new WebHook instance.

Parameters:
- `name string` - WebHook name
- `item map[string]interface{}` - WebHook configuration, including the following fields:
  - `type` - WebHook type
  - `msg_types` - List of supported message types
  - `accounts` - List of supported accounts
  - `keywords` - Keyword filter list
  - `retry_num` - Number of retries
  - `retry_delay` - Retry interval (seconds)
  - `disable` - Whether disabled

Returns:
- `*WebHook` - WebHook instance

### NewWeWork
Create a new WeCom (WeChat Work) WebHook instance.

Parameters:
- `name string` - WebHook name
- `item map[string]interface{}` - WeCom configuration, including the following fields:
  - `corp_id` - Enterprise ID
  - `corp_secret` - Application secret
  - `agent_id` - Application ID
  - `touser` - Message recipients (users)
  - `toparty` - Message recipients (departments)
  - `totag` - Message recipients (tags)

Returns:
- `*WeWork` - WeCom WebHook instance 