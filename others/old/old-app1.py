
from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from flask import Flask, redirect, render_template, request, url_for

app = Flask(__name__)


import paho.mqtt.client as mqtt
import json
from threading import Thread

# ... (seu código de alunos e registros continua igual)

# ---------------- CONFIGURAÇÃO MQTT ----------------
MQTT_BROKER = "brw.net.br"
MQTT_TOPIC = "aluno/id"

def ao_receber_mensagem(client, userdata, msg):
    uid_recebido = msg.payload.decode().strip()
    print(f"MQTT: UID recebido -> {uid_recebido}")

    aluno = buscar_aluno_por_uid(uid_recebido)
    tipo = definir_tipo_registro(aluno["id_aluno"]) if aluno else "erro"

    novo_registro = {
        "id_aluno": aluno["id_aluno"] if aluno else "-",
        "nome": aluno["nome"] if aluno else "Cartão não identificado",
        "uid": uid_recebido,
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "tipo": tipo,
    }

    registros.append(novo_registro)
    print(f"Registro adicionado para: {novo_registro['nome']}")

def iniciar_mqtt():
    cliente = mqtt.Client()
    cliente.username_pw_set("brware", "SQRT(pi)!=314")
    cliente.on_message = ao_receber_mensagem
    cliente.connect(MQTT_BROKER, 1883)
    cliente.subscribe(MQTT_TOPIC)
    cliente.loop_forever()

# Base simulada de alunos
alunos: List[Dict[str, str]] = [
    {"id_aluno": "1", "nome": "Julia Augustinho", "id_nfc": "UID123"},
    {"id_aluno": "2", "nome": "Amanda", "id_nfc": "UID456"},
    {"id_aluno": "3", "nome": "Giovanna", "id_nfc": "UID789"},
]

# Registros de presença
registros: List[Dict[str, str]] = []


def buscar_aluno_por_uid(uid: str) -> Optional[Dict[str, str]]:
    """Busca um aluno pelo UID do cartão NFC."""
    for aluno in alunos:
        if aluno["id_nfc"] == uid:
            return aluno
    return None


def definir_tipo_registro(id_aluno: str) -> str:
    """
    Define se o próximo registro será entrada ou saída.
    Regra:
    - se não houver registro anterior, será entrada
    - se o último registro foi entrada, o próximo será saída
    - caso contrário, será entrada
    """
    registros_aluno = [r for r in registros if r["id_aluno"] == id_aluno]

    if not registros_aluno:
        return "entrada"

    ultimo = registros_aluno[-1]
    return "saida" if ultimo["tipo"] == "entrada" else "entrada"


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html", alunos=alunos, registros=registros)

@app.route("/cadastrar_aluno", methods=["POST"])
def cadastrar_aluno():
    id_aluno = request.form.get("id_aluno", "").strip()
    nome = request.form.get("nome", "").strip()
    id_nfc = request.form.get("id_nfc", "").strip()

    if id_aluno and nome and id_nfc:
        # Evita duplicidade de UID
        if not any(aluno["id_nfc"] == id_nfc for aluno in alunos):
            alunos.append(
                {
                    "id_aluno": id_aluno,
                    "nome": nome,
                    "id_nfc": id_nfc,
                }
            )

    return redirect(url_for("index"))


@app.route("/registrar_presenca", methods=["POST"])
def registrar_presenca():
    uid = request.form.get("uid", "").strip()

    if not uid:
        return redirect(url_for("index"))

    aluno = buscar_aluno_por_uid(uid)

    if aluno is not None:
        tipo = definir_tipo_registro(aluno["id_aluno"])

        registros.append(
            {
                "id_aluno": aluno["id_aluno"],
                "nome": aluno["nome"],
                "uid": uid,
                "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "tipo": tipo,
            }
        )
    else:
        registros.append(
            {
                "id_aluno": "-",
                "nome": "Cartão não identificado",
                "uid": uid,
                "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "tipo": "erro",
            }
        )

    return redirect(url_for("index"))


@app.route("/limpar_registros", methods=["POST"])
def limpar_registros():
    registros.clear()
    return redirect(url_for("index"))


if __name__ == "__main__":
    print("Servidor iniciando em http://127.0.0.1:5000")
    app.run(host="127.0.0.1", port=5000, debug=True)

# Inicia o MQTT em uma Thread separada para não travar o Flask
    thread_mqtt = Thread(target=iniciar_mqtt)
    thread_mqtt.daemon = True
    thread_mqtt.start()

    app.run(host="127.0.0.1", port=5000, debug=True)

