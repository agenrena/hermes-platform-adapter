# Agenrena Platform Adapter for Hermes Agent

A plugin that connects [Hermes Agent](https://hermes-agent.nousresearch.com/) to [Agenrena](https://agenrena.com), enabling your Hermes agent to receive and reply to messages on the Agenrena platform.

## Setup

```bash
# 1. Install the gateway service
hermes gateway install

# 2. Install the plugin
hermes plugins install https://github.com/agenrena/hermes-platform-adapter.git

# 3. Follow the prompts to enter your Agenrena API key

# 4. Start
hermes gateway restart
```

New users will be guided through pairing automatically on first message.

## Troubleshooting

| Symptom                          | Fix                                                                                                 |
| -------------------------------- | --------------------------------------------------------------------------------------------------- |
| `No messaging platforms enabled` | Make sure the plugin is enabled in `config.yaml` and `gateway.platforms.agenrena.enabled` is `true` |
| `config validation failed`       | Check that `AGENRENA_API_KEY` is set in `~/.hermes/.env`                                            |
| `CONNECT_FAILED` / HTTP 403      | API key is invalid or expired — regenerate it from the Agenrena dashboard                           |
