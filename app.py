from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from flask import Flask, redirect, render_template, request, url_for

import paho.mqtt.client as mqtt
#import json
from threading import Thread
# import logging

app = Flask(__name__)


# ... (seu código de alunos e registros continua igual)
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)
# ---------------- CONFIGURAÇÃO MQTT ----------------
MQTT_BROKER = "brw.net.br"
MQTT_TOPIC = "aluno/id"

def ao_receber_mensagem(client, userdata, msg):
# logger.debug(f"[MQTT] Topico: {msg.topic}, mensagem: {msg.payload.decode()}")    

    global ultimo_uid_cadastro, ultimo_uid_presenca

    uid_recebido = msg.payload.decode().strip()

    # Alterna entre cadastro e presença para não conflitar
    # Se o último foi para cadastro, o próximo vai para presença
    global _ultimo_para_cadastro
    if not hasattr(ao_receber_mensagem, '_ultimo_para_cadastro'):
        ao_receber_mensagem._ultimo_para_cadastro = True

    if ao_receber_mensagem._ultimo_para_cadastro:
        ultimo_uid_cadastro = uid_recebido
        ao_receber_mensagem._ultimo_para_cadastro = False
    else:
        ultimo_uid_presenca = uid_recebido
        ao_receber_mensagem._ultimo_para_cadastro = True
    # aluno = buscar_aluno_por_uid(uid_recebido)
    # tipo = definir_tipo_registro(aluno["id_aluno"]) if aluno else "erro"

    # novo_registro = {
    #     "id_aluno": aluno["id_aluno"] if aluno else "-",
    #     "nome": aluno["nome"] if aluno else "Cartão não identificado",
    #     "uid": uid_recebido,
    #     "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    #     "tipo": tipo,
    # }

    # registros.append(novo_registro)
    # print(f"Registro adicionado para: {novo_registro['nome']}")
    # print(f"Lista de chamada: {registros}")

def iniciar_mqtt():
    cliente = mqtt.Client()
    # cliente.username_pw_set("brware", "SQRT(pi)!=314") liberou o acesso anonimo
    cliente.on_message = ao_receber_mensagem
    cliente.connect(MQTT_BROKER, 1883)
    if cliente.connect: 
        print('Conectado ao:', MQTT_BROKER)
    else:
         print('NAO Conectado:')
    cliente.subscribe(MQTT_TOPIC)
    print('Inscrito no tópico: ', MQTT_TOPIC)
    cliente.loop_forever()

# Base simulada de alunos
alunos: List[Dict[str, str]] = [
    {"id_aluno": "1", "nome": "Julia Augustinho", "id_nfc": "A8 10 15 C1 18"},
    {"id_aluno": "2", "nome": "Amanda", "id_nfc": "UID456"},
    {"id_aluno": "3", "nome": "Giovanna", "id_nfc": "UID789"},
]

# Registros de presença
registros: List[Dict[str, str]] = []
ultimo_uid_cadastro = ""
ultimo_uid_presenca = ""


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


def calcular_lista_presenca() -> List[Dict[str, str]]:
    """Retorna a lista de presença atual de cada aluno."""
    lista_presenca: List[Dict[str, str]] = []

    for aluno in alunos:
        registros_aluno = [r for r in registros if r["id_aluno"] == aluno["id_aluno"] and r["tipo"] in {"entrada", "saida"}]
        if registros_aluno:
            ultimo = registros_aluno[-1]
            status = "Presente" if ultimo["tipo"] == "entrada" else "Ausente"
            ultima_hora = ultimo["data_hora"]
        else:
            status = "Ausente"
            ultima_hora = "-"

        lista_presenca.append(
            {
                "id_aluno": aluno["id_aluno"],
                "nome": aluno["nome"],
                "id_nfc": aluno["id_nfc"],
                "status": status,
                "ultima_hora": ultima_hora,
            }
        )

    return lista_presenca


@app.route("/", methods=["GET"])
def index():
    lista_presenca = calcular_lista_presenca()
    presentes = sum(1 for item in lista_presenca if item["status"] == "Presente")

    return render_template(
        "index.html",
        alunos=alunos,
        registros=registros,
        lista_presenca=lista_presenca,
        total_presentes=presentes,
    )

@app.route("/cadastro", methods=["GET"])
def cadastro_page():
    return render_template(
        "cadastro.html",
        alunos=alunos,
        ultimo_uid_cadastro=ultimo_uid_cadastro,
    )

@app.route("/presenca", methods=["GET"])
def presenca_page():
    lista_presenca = calcular_lista_presenca()
    presentes = sum(1 for item in lista_presenca if item["status"] == "Presente")

    return render_template(
        "presenca.html",
        registros=registros,
        lista_presenca=lista_presenca,
        total_presentes=presentes,
        ultimo_uid_presenca=ultimo_uid_presenca,
    )

@app.route("/cadastrar_aluno", methods=["POST"])
def cadastrar_aluno():
    global ultimo_uid_cadastro
    
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
            # Limpa o UID após cadastro
            ultimo_uid_cadastro = ""

    return redirect(url_for("cadastro_page"))


@app.route("/registrar_presenca", methods=["POST"])
def registrar_presenca():
    global ultimo_uid_presenca
    
    uid = request.form.get("uid", "").strip()

    if not uid:
        return redirect(url_for("presenca_page"))

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
    
    # Limpa o UID após registrar
    ultimo_uid_presenca = ""

    return redirect(url_for("presenca_page"))


@app.route("/limpar_registros", methods=["POST"])
def limpar_registros():
    registros.clear()
    return redirect(url_for("presenca_page"))


if __name__ == "__main__":
    #print("Servidor iniciando em http://127.0.0.1:5000")
    thread_mqtt = Thread(target=iniciar_mqtt, daemon=True)
    thread_mqtt.start()
    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
    #iniciar_mqtt()    
   

# Inicia o MQTT em uma Thread separada para não travar o Flask
    # # thread_mqtt.daemon = True
    # thread_mqtt.start()