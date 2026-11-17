"""
Relatório de saúde no terminal, sem precisar do Claude.

  cm-health-report                      # texto legível, todos os clusters
  cm-health-report --json               # JSON (para scripts, cron, CI)
  cm-health-report --cluster LAB --hours 12 --env-file .env

Código de saída: 0 = OK, 1 = ATENCAO, 2 = CRITICO, 3 = erro de conexão/configuração.
As credenciais vêm das variáveis CM_* (veja .env.example) ou de --env-file.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

EXIT_CODES = {"OK": 0, "ATENCAO": 1, "CRITICO": 2}


def _load_env_file(path: str) -> None:
    """Carrega KEY=VALUE de um arquivo .env sem sobrescrever o que já está no ambiente."""
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="cm-health-report",
                                 description="Relatório de saúde do Cloudera Manager (somente leitura).")
    ap.add_argument("--cluster", help="cluster específico (padrão: todos)")
    ap.add_argument("--hours", type=float, default=24, help="janela dos alertas em horas (padrão 24)")
    ap.add_argument("--heartbeat-seconds", type=int, default=120,
                    help="idade máxima do heartbeat do agente (padrão 120)")
    ap.add_argument("--stuck-minutes", type=float, default=60,
                    help="minutos para considerar um comando travado (padrão 60)")
    ap.add_argument("--json", action="store_true", help="saída em JSON")
    ap.add_argument("--env-file", help="arquivo .env com as variáveis CM_*")
    args = ap.parse_args(argv)

    if args.env_file:
        _load_env_file(args.env_file)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    # a configuração é lida na importação do pacote, então o .env vem antes
    from cm_mcp_server.health import build_health_report, render_text

    try:
        report = build_health_report(args.cluster, args.hours, args.heartbeat_seconds,
                                     stuck_command_minutes=args.stuck_minutes)
    except Exception as e:  # noqa: BLE001
        print(f"Erro ao consultar o Cloudera Manager: {e}", file=sys.stderr)
        return 3
    print(json.dumps(report, ensure_ascii=False, indent=2) if args.json else render_text(report), end="")
    if args.json:
        print()
    return EXIT_CODES[report["overallStatus"]]


def license_main(argv: list[str] | None = None) -> int:
    """
    cm-license-report --cluster SPtrans                 # grava HTML em ~/cm-reports
    cm-license-report --cluster SPtrans --format pdf    # HTML + PDF (Edge/Chrome headless)
    cm-license-report --cluster SPtrans --format json   # JSON completo no stdout
    """
    ap = argparse.ArgumentParser(prog="cm-license-report",
                                 description="Levantamento de capacidade e licenciamento Cloudera (CCU, DUM, CUM).")
    ap.add_argument("--cluster", required=True, help="nome interno do cluster")
    ap.add_argument("--format", choices=("html", "pdf", "json"), default="html")
    ap.add_argument("--days", type=float, default=7, help="janela de uso em dias (padrão 7)")
    ap.add_argument("--out", help="pasta de saída (padrão CM_REPORT_DIR ou ~/cm-reports)")
    ap.add_argument("--data-mounts", help="regex dos pontos de montagem de dados")
    ap.add_argument("--nominal-step", type=int, default=8, help="múltiplo em GB da RAM nominal (padrão 8)")
    ap.add_argument("--env-file", help="arquivo .env com as variáveis CM_*")
    args = ap.parse_args(argv)

    if args.env_file:
        _load_env_file(args.env_file)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    from cm_mcp_server.licensing import DEFAULT_DATA_MOUNTS, build_license_report, write_report

    try:
        report = build_license_report(args.cluster, args.days, args.data_mounts or DEFAULT_DATA_MOUNTS,
                                      args.nominal_step)
    except Exception as e:  # noqa: BLE001
        print(f"Erro ao consultar o Cloudera Manager: {e}", file=sys.stderr)
        return 3
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    try:
        paths = write_report(report, args.format, args.out)
    except Exception as e:  # noqa: BLE001
        print(f"Erro ao gravar o relatório: {e}", file=sys.stderr)
        return 4
    o = report["ccu"]["os"]
    print(f"{report['cluster']}: {report['totals']['nodes']} nodes, {o['total']} CCU (RAM do SO), "
          f"{report['ccu']['nominal']['total']} CCU (RAM nominal)")
    for kind, path in paths.items():
        print(f"{kind.upper()}: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
