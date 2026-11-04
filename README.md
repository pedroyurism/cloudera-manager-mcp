# Cloudera Manager MCP Server (somente leitura)

Servidor MCP em Python (FastMCP + httpx) que expõe a API REST do Cloudera Manager
para o Claude (Desktop, Claude Code ou qualquer cliente MCP). **Só faz GET**: não há
nenhuma tool de start/stop/restart/alteração de configuração.

## Tools (26)

### Cloudera Manager (18)

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

### Monitoramento YARN (8)

| Tool | Fonte | O que faz |
|---|---|---|
| `yarn_list_applications` | CM | Jobs rodando, terminados ou todos; filtra por usuário, fila e janela; ordena por memória atual, GB-hora, vcore-hora ou duração |
| `yarn_top_consumers` | CM | **Ranking de quem mais consome** (por usuário ou por fila): GB-hora, vcore-hora, % do total, uso agora, falhas, job mais pesado |
| `yarn_queue_usage` | CM | Uso atual e pico de cada fila: memória/vcores alocados e pendentes, apps rodando/pendentes, fair share (Fair Scheduler) |
| `yarn_utilization_report` | CM | Relatório de utilização por fila ou usuário (médias, capacidade ociosa, espera em contenção), com recorte de horário e dias úteis |
| `yarn_application_attributes` | CM | Atributos aceitos em `extra_filter` (ex.: `application_duration > 30m`) |
| `yarn_scheduler_queues` | RM | Árvore de filas: capacidade configurada x usada e **consumo de cada usuário dentro da fila** |
| `yarn_cluster_metrics` | RM | Memória/vcores totais e alocados (%), apps e containers pendentes, NodeManagers com problema |
| `yarn_get_application` | RM | Detalhe de um job, incluindo `diagnostics` (o motivo da falha) |

As tools marcadas **CM** usam só as credenciais do Cloudera Manager. As marcadas **RM**
falam direto com o ResourceManager (veja [Acesso ao ResourceManager](#acesso-ao-resourcemanager)).

As respostas são resumidas (campos relevantes) e truncadas em `CM_MCP_MAX_CHARS`
para não estourar o contexto do modelo.

## Pré-requisitos

- Python 3.10+
- Acesso de rede da máquina que roda o MCP até o CM (porta 7183/7180)
- **Usuário dedicado no CM com role `Read-Only`** (Administration → Users & Roles)

## Instalação

```bash
git clone https://github.com/pedroyurism/cloudera-manager-mcp.git
cd cloudera-manager-mcp
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
Reinicie o Claude Desktop e as 26 tools aparecem no menu de ferramentas.

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

## Acesso ao ResourceManager

As tools `yarn_scheduler_queues`, `yarn_cluster_metrics` e `yarn_get_application` usam a API
REST do RM (`/ws/v1`). Por padrão o servidor **descobre o RM ativo pelo CM** (role
`RESOURCEMANAGER` com `haStatus=ACTIVE` e a porta web configurada), então em clusters sem
Kerberos na interface web não é preciso configurar nada.

| Cenário | Configuração |
|---|---|
| Web UI do RM sem autenticação | nada (padrão `YARN_RM_AUTH=none`) |
| Kerberos/SPNEGO na web UI | `pip install -e '.[kerberos]'`, `kinit` com um principal válido e `YARN_RM_AUTH=kerberos` |
| Acesso via Knox | `YARN_RM_URL=https://knox:8443/gateway/cdp-proxy-api/resourcemanager`, `YARN_RM_AUTH=basic`, `YARN_RM_USERNAME`/`YARN_RM_PASSWORD` |

Se o RM não estiver acessível, as outras 23 tools continuam funcionando normalmente.

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
| `YARN_RM_URL` | descobrir pelo CM | URL base do RM ou do Knox, ex.: `https://rm01:8090` |
| `YARN_RM_TLS` | igual a `CM_USE_TLS` | Usado na descoberta: HTTPS (8090) ou HTTP (8088) |
| `YARN_RM_PORT` | config da role | Força a porta na descoberta |
| `YARN_RM_AUTH` | `none` | `none`, `basic` (Knox) ou `kerberos` |
| `YARN_RM_USERNAME` / `YARN_RM_PASSWORD` | — | Só para `YARN_RM_AUTH=basic` |
| `YARN_RM_VERIFY_SSL` | igual a `CM_VERIFY_SSL` | `true`, `false` ou caminho do CA bundle |

## Exemplos de perguntas

- "Quais serviços do Cluster1 não estão saudáveis e por quê?"
- "Mostra as últimas 300 linhas do stderr do ResourceManager que está com problema."
- "Quais configs do Impala foram alteradas em relação ao default?"
- "Qual foi o pico de CPU dos hosts na última hora?" (tsquery `select cpu_percent where category = HOST`)
- "Teve algum evento CRITICAL de HDFS nas últimas 12 horas?"
- "Quem reiniciou serviços ontem?" (auditoria)

**YARN**

- "Quais 10 usuários mais consumiram memória do YARN nas últimas 24 horas?"
- "Quais filas estão com containers pendentes agora?"
- "Mostra os jobs rodando há mais de 2 horas na fila root.etl."
- "Como está a ocupação de cada fila em relação à capacidade configurada, e quem está usando cada uma?"
- "Relatório de utilização por usuário da última semana, só em horário comercial de dias úteis."
- "Por que o job application_1700000000000_0400 falhou?"

## Segurança

- O código só executa `GET`. Mesmo assim, use um usuário Read-Only: é a garantia real.
- Logs e configs podem conter dados sensíveis do cliente e vão para o contexto do
  modelo. Avalie com o cliente antes de usar em produção.
- Configurações marcadas como `sensitive` no CM já vêm mascaradas pela própria API.
- Não versione o `.env`.

## Próximos passos possíveis

- Tools de escrita (restart de role, refresh de config) atrás de uma flag
  `CM_ALLOW_WRITE=true` e confirmação explícita.
- Spark History Server e Impala (queries mais pesadas por usuário) para diagnóstico de jobs.

## Desenvolvimento

```bash
pip install -e '.[dev]'
pytest -q        # roda as tools contra um CM e um RM simulados (tests/mock_servers.py)
```

## Licença

Distribuído sob a [Apache License 2.0](LICENSE). Veja também o arquivo [NOTICE](NOTICE).

Cloudera e Cloudera Manager são marcas da Cloudera, Inc. Este projeto não é afiliado
nem endossado pela Cloudera.
