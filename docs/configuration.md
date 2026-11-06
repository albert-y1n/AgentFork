# Configuration

Run from a source checkout, or set `AGENTFORK_HOME` to an explicitly configured workspace. Runtime state belongs under that workspace and is ignored by Git.

| Variable | Meaning |
| --- | --- |
| `AGENTFORK_HOME` | Workspace containing `tools`, `runtime`, `datasets`, `runs` and `storage` |
| `AGENTFORK_TASKS` | Directory containing Harbor task subdirectories |
| `AGENTFORK_CLAUDE_BINARY` | Native Linux Claude executable to copy into task containers |
| `AGENTFORK_MODEL` | Exact model name for actor and review requests |
| `AGENTFORK_BASE_URL` | Provider origin; actor uses `/v1/messages`, review uses `/v1/chat/completions` |
| `AGENTFORK_API_KEY` | Provider credential, read from the environment |
| `AGENTFORK_HARBOR_PYTHON` | Interpreter with the optional pinned Harbor dependency installed; defaults to this interpreter |
| `AGENTFORK_RUNTIME_BINARY` | Static runtime executable, default `runtime/agentfork-runtime` |
| `AGENTFORK_MAX_OUTPUT_TOKENS` | Actor output limit, default 128000 |
| `AGENTFORK_REVIEW_MAX_OUTPUT_TOKENS` | Review output limit, default 128000 |
| `AGENTFORK_CONTEXT_TOKENS` | Claude harness context limit, default 1000000; must match provider capability |

CLI `--concurrency` limits continuations within a task; `--task-workers` controls simultaneous tasks. Choose both for the available CPU, memory, provider limits and checkpoint-storage headroom. More branches can increase throughput while also increasing transient memory and disk demand.

The viewer binds to `127.0.0.1:18765` by default. The included server provides no public authentication layer. Put an authentication-capable reverse proxy in front of it before exposing private trajectories; public hosting, DNS, TLS credentials and machine-specific configuration are deliberately not included.
