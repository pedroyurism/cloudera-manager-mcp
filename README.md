# Cloudera Manager MCP Server (somente leitura)

Servidor MCP em Python (FastMCP + httpx) que expõe a API REST do Cloudera Manager
para o Claude (Desktop, Claude Code ou qualquer cliente MCP). **Só faz GET**: não há
nenhuma tool de start/stop/restart/alteração de configuração.

## Tools (18)

| Tool | O que faz |
|---|---|
| `cm_info` | Versão do CM, versão da API e saúde do Management Service |
| `list_clusters` | Clusters, versão do runtime, status |
| `list_services` | Serviços do cluster com estado, saúde, stale config |
| `cluster_health_summary` | Só o que **não** está GOOD: serviços, roles e checks que falharam |
| `get_service` | Detalhe de um serviço e seus health checks |
| `list_roles` | Roles de um serviço (opção `only_unhealthy`) |
| `get_service_config` | Configs de serviço (padrão: só valores alterados) |
| `get_role_config_groups` | Role config groups e suas configs alteradas |
| `get_role_log` | Últimas N linhas de `full`, `stderr` ou `stdout` de uma role |
| `list_hosts` / `get_host` | Inventário de hosts, heartbeat, roles hospedadas |
| `query_timeseries` | Executa uma **tsquery** e devolve min/max/último valor |
| `list_events` | Eventos (por severidade, categoria, serviço, janela em horas) |
| `list_active_commands` / `get_command` | Comandos em execução e subcomandos que falharam |
| `list_parcels` | Parcels e estágio (ACTIVATED, DISTRIBUTED…) |
| `list_audits` | Auditoria do CM (quem fez o quê) |
| `cm_api_get` | Escape hatch: GET em qualquer endpoint da API |

As respostas são resumidas (campos relevantes) e truncadas em `CM_MCP_MAX_CHARS`
para não estourar o contexto do modelo.

## Pré-requisitos

- Python 3.10+
- Acesso de rede da máquina que roda o MCP até o CM (porta 7183/7180)
- **Usuário dedicado no CM com role `Read-Only`** (Administration → Users & Roles)

## Instalação

```bash
git clone <seu-repo>/cm-mcp-server.git   # ou descompacte o zip
cd cm-mcp-server
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env    # edite com seus valores
```

Teste rápido de conectividade com o CM antes de plugar no Claude:

```bash
curl -su mcp_readonly https://cm01.cliente.local:7183/api/version
```

## Claude Desktop (stdio)

Edite o `claude_desktop_config.json`
(macOS: `~/Library/Application Support/Claude/`, Windows: `%APPDATA%\Claude\`):

```json
{
  "mcpServers": {
    "cloudera-manager": {
      "command": "/caminho/absoluto/cm-mcp-server/.venv/bin/cm-mcp-stdio",
      "env": {
        "CM_HOST": "cm01.cliente.local",
        "CM_USE_TLS": "true",
        "CM_VERIFY_SSL": "/caminho/para/ca-bundle.pem",
        "CM_USERNAME": "mcp_readonly",
        "CM_PASSWORD": "********"
      }
    }
  }
}
```

No Windows o comando fica `C:\\caminho\\cm-mcp-server\\.venv\\Scripts\\cm-mcp-stdio.exe`.
Reinicie o Claude Desktop e as 18 tools aparecem no menu de ferramentas.

## Claude Code

```bash
claude mcp add cloudera-manager \
  -e CM_HOST=cm01.cliente.local -e CM_USERNAME=mcp_readonly -e CM_PASSWORD=*** \
  -- /caminho/absoluto/cm-mcp-server/.venv/bin/cm-mcp-stdio
```

## Modo HTTP (opcional)

```bash
set -a; source .env; set +a
cm-mcp-http      # escuta em http://127.0.0.1:8000/mcp
```

⚠️ O modo HTTP **não tem autenticação própria**. Mantenha em `127.0.0.1` ou coloque
atrás de um reverse proxy com TLS e autenticação antes de expor na rede. Para usar como
connector remoto no claude.ai, o endpoint precisa estar acessível pela internet, o que
normalmente não é aceitável para o CM de um cliente; prefira stdio local.

## Variáveis de ambiente

| Variável | Padrão | Descrição |
|---|---|---|
| `CM_HOST` | — | Host do CM (sem `https://` e sem porta) |
| `CM_PORT` | 7183 / 7180 | Porta (depende de `CM_USE_TLS`) |
| `CM_USE_TLS` | `true` | HTTPS ou HTTP |
| `CM_VERIFY_SSL` | `true` | `true`, `false` ou caminho do CA bundle |
| `CM_USERNAME` / `CM_PASSWORD` | — | Usuário Read-Only do CM |
| `CM_API_VERSION` | autodetectar | Ex.: `v51`. Vazio = consulta `/api/version` |
| `CM_TIMEOUT` | `30` | Timeout em segundos |
| `CM_MCP_MAX_CHARS` | `60000` | Tamanho máximo de cada resposta |

## Exemplos de perguntas

- "Quais serviços do Cluster1 não estão saudáveis e por quê?"
- "Mostra as últimas 300 linhas do stderr do ResourceManager que está com problema."
- "Quais configs do Impala foram alteradas em relação ao default?"
- "Qual foi o pico de CPU dos hosts na última hora?" (tsquery `select cpu_percent where category = HOST`)
- "Teve algum evento CRITICAL de HDFS nas últimas 12 horas?"
- "Quem reiniciou serviços ontem?" (auditoria)

## Segurança

- O código só executa `GET`. Mesmo assim, use um usuário Read-Only: é a garantia real.
- Logs e configs podem conter dados sensíveis do cliente e vão para o contexto do
  modelo. Avalie com o cliente antes de usar em produção.
- Configurações marcadas como `sensitive` no CM já vêm mascaradas pela própria API.
- Não versione o `.env`.

## Próximos passos possíveis

- Tools de escrita (restart de role, refresh de config) atrás de uma flag
  `CM_ALLOW_WRITE=true` e confirmação explícita.
- Integrações com YARN RM / Spark History / Impala Daemon web UIs para diagnóstico de jobs.

## Licença

Distribuído sob a [Apache License 2.0](LICENSE). Veja também o arquivo [NOTICE](NOTICE).

Cloudera e Cloudera Manager são marcas da Cloudera, Inc. Este projeto não é afiliado
nem endossado pela Cloudera.
