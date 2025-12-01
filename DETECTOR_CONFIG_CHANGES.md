# Detector Configuration Updates

## Summary

The detector configuration has been significantly enhanced to provide full flexibility and support for different MQTT communication patterns.

## Key Changes

### 1. Fully Configurable Topics
- **Before**: Topics were constructed from `topic_base_detector` and `device_name_detector`
- **After**: Each topic is fully configurable:
  - `topic_cmnd`: Command topic for triggering measurements
  - `topic_response`: Response topic for receiving measurements
  - `topic_error`: Error topic for error messages
  - `topic_settings_cmnd`: Command topic for settings
  - `topic_settings_response`: Response topic for settings responses

### 2. Configurable Payload Key
- **Before**: Hardcoded to use `"value"` key in JSON payload
- **After**: Configurable via `payload_key` parameter
- **Example**: Can now use payloads like `{"signal": 123.45}` by setting `payload_key: "signal"`

### 3. Optional Confirmation Logic
Two modes are now supported:

#### Confirmation Mode (use_confirmation: true)
- Request/response pattern with confirmation IDs
- Waits for confirmation before returning value
- Configurable timeout via `confirmation_timeout`
- Best for: Command-driven measurements

#### Listen-Only Mode (use_confirmation: false)
- Subscribes to topic and reads latest value
- No confirmation or waiting required
- Best for: Continuous data streams, polling scenarios
- No timeout needed

## Configuration Examples

### Confirmation Mode (Default)
```yaml
detector:
  topic_cmnd: "device/cmnd"
  topic_response: "device/response"
  topic_error: "device/error"
  topic_settings_cmnd: "device/settings/cmnd"
  topic_settings_response: "device/settings/response"
  payload_key: "value"
  use_confirmation: true
  confirmation_timeout: 2
```

### Listen-Only Mode
```yaml
detector:
  topic_cmnd: "device/trigger"
  topic_response: "device/stream"
  topic_error: "device/error"
  topic_settings_cmnd: "device/config"
  topic_settings_response: "device/config/response"
  payload_key: "signal_value"
  use_confirmation: false
```

## Backward Compatibility

The system maintains backward compatibility with legacy configuration:
```yaml
topic_base_detector: "lqt/picoscope"
device_name_detector: "trigger_rate"
```

If the new `detector` section is not present, the system will automatically:
- Construct topics using the legacy pattern
- Use `"value"` as the payload key
- Enable confirmation mode with 2-second timeout

## Implementation Details

### Files Modified
- `config.yaml`: Added new detector configuration section
- `ms_logic.py`: 
  - Updated `__init__` to parse new configuration
  - Modified `on_connect` to subscribe to configurable topics
  - Updated `on_message` to route messages using configurable topics
  - Enhanced `handle_response_detector` to use configurable payload key
  - Modified `publish_measure_signal` to support both modes
  - Updated `measure_signal` to handle optional confirmation
  - Updated `configure_detector` to use configurable topics

### New Features
- `latest_detector_value`: Stores the most recent detector value for listen-only mode
- Conditional confirmation logic based on `use_confirmation` setting
- Type-safe handling of optional confirmation IDs

## Migration Guide

To upgrade from legacy configuration to the new format:

1. Add the `detector` section to your `config.yaml`
2. Set topics according to your MQTT broker setup
3. Choose your mode:
   - Set `use_confirmation: true` for request/response pattern
   - Set `use_confirmation: false` for listen-only mode
4. Set `payload_key` to match your detector's JSON structure
5. Optionally adjust `confirmation_timeout` for your network latency

The legacy configuration will continue to work without changes.
