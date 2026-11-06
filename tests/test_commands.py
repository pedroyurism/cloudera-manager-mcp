"""
Testes de comandos em andamento e travados (list_running_commands, get_command e o relatório).

Cenário do CM simulado: um restart do HDFS disparado direto no serviço, rodando há 3 h e parado
em "Start DataNode" (hdfs-DN-2 em node02.local), e um restart do cluster com 10 min, que
aparece em dois níveis (cluster e impala) e deve ser contado uma vez só.
"""

from __future__ import annotations

from tests.test_tools import call, ok  # importa o ambiente e sobe os servidores simulados


def test_running_commands_cover_all_levels():
    data = ok("list_running_commands", cluster="Cluster1")
    # cm/commands, cm/service/commands, cluster e os 4 serviços
    assert data["sourcesChecked"] == 7
    assert data["running"] == 2 and data["stuck"] == 1
    ids = [c["id"] for c in data["commands"]]
    assert ids == [20, 7]                                      # mais antigo primeiro, sem duplicar o 7
    assert "errors" not in data


def test_stuck_command_shows_where_it_stopped():
    stuck = ok("list_running_commands", cluster="Cluster1")["commands"][0]
    assert stuck["stuck"] is True and stuck["scope"] == "service"
    assert stuck["service"] == "hdfs" and stuck["elapsedMin"] >= 179
    assert stuck["activeSteps"] == ["Start", "Start DataNode"]
    step = stuck["currentStep"]
    assert step["id"] == 23 and step["role"] == "hdfs-DN-2" and step["host"] == "node02.local"


def test_stuck_threshold_is_configurable():
    data = ok("list_running_commands", cluster="Cluster1", stuck_minutes=240)
    assert data["stuck"] == 0 and data["stuckMinutes"] == 240
    data = ok("list_running_commands", cluster="Cluster1", stuck_minutes=5)
    assert data["stuck"] == 2


def test_role_level_is_optional():
    data = ok("list_running_commands", cluster="Cluster1", include_roles=True)
    assert data["sourcesChecked"] > 7                          # mais as listas de cada role
    assert data["running"] == 2                                # rotas sem comandos (404) não derrubam a tool
    assert data["errors"]


def test_no_running_commands_message():
    data = ok("list_running_commands", cluster="ClusterSemNada")
    assert data["commands"] == "Nenhum comando em andamento."
    assert data["errors"]                                      # o cluster não existe no mock


def test_get_command_shows_active_and_failed_children():
    running = ok("get_command", command_id=20)
    assert running["elapsedMin"] >= 179
    assert [c["id"] for c in running["activeChildren"]] == [22]
    assert running["failedChildren"] == []
    assert running["currentStep"]["name"] == "Start DataNode"
    finished = ok("get_command", command_id=7)                 # já terminou, com subcomando que falhou
    assert finished["failedChildren"][0]["role"] == "yarn-RM-1"
    assert "currentStep" not in finished


def test_list_active_commands_still_works():
    data = ok("list_active_commands", cluster="Cluster1")
    assert data[0]["id"] == 7


def test_health_report_flags_stuck_command():
    report = ok("cluster_health_report", cluster="Cluster1")
    assert report["counters"]["commands"] == {"running": 2, "stuck": 1}
    stuck = [f for f in report["findings"] if f["area"] == "comandos"]
    assert len(stuck) == 1 and stuck[0]["level"] == "atencao"
    assert "#20" in stuck[0]["entity"] and "Start DataNode em hdfs-DN-2" in stuck[0]["message"]
    text = ok("cluster_health_report", cluster="Cluster1", output="text", stuck_command_minutes=240)
    assert "Comandos em andamento: 2 (travados 0, limite 240 min)" in text
    _, msg = call("cluster_health_report", cluster="Cluster1", output="text")
    assert "TRAVADO #20 Restart em Cluster1/hdfs" in msg
