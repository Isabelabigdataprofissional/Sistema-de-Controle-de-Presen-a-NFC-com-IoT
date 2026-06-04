from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional

from flask import Flask, redirect, render_template, request, url_for, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

import paho.mqtt.client as mqtt
#import json
import time
from queue import Queue, Empty
from threading import Thread, Lock
# import logging

from models import Base, Aluno, Presenca

app = Flask(__name__)

# ===== CONFIGURAÇÃO DO BANCO DE DADOS =====
DATABASE_URL = "sqlite:///presenca.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Cria as tabelas automaticamente se não existirem
Base.metadata.create_all(bind=engine)
print("[DB] Tabelas criadas/verificadas com sucesso")


def get_db_session() -> Session:
    """Retorna uma nova sessão de banco de dados."""
    return SessionLocal()

# ===== NOVA FUNCIONALIDADE: FILA DE PROCESSAMENTO NFC =====
fila_uids: Queue[Dict[str, str]] = Queue()
historico_leituras: List[Dict[str, str]] = []
ultimo_evento_presenca: Dict[str, str] = {
    "uid": "",
    "id_aluno": "",
    "nome": "",
    "data_hora": "",
    "situacao": "",
    "tipo": "",
}
ultimo_uid_cadastro = ""
ultimo_uid_presenca = ""
ultimo_para_cadastro = True
estado_lock = Lock()
IGNORE_DUPLICATE_INTERVAL_SEGUNDOS = 5
ultimas_leituras_por_uid: Dict[str, datetime] = {}


# ... (seu código de alunos e registros continua igual)
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)
# ---------------- CONFIGURAÇÃO MQTT ----------------
MQTT_BROKER = "brw.net.br"
MQTT_TOPIC = "aluno/id"
MQTT_USERNAME = "brware"
MQTT_PASSWORD = "SQRT(pi)!=314"

# ===== ALTERAÇÃO: NORMALIZAÇÃO DE UID NFC =====
def normalizar_uid(uid: str) -> str:
    """Normaliza o UID para comparação e armazenamento uniforme."""
    if not uid:
        return ""

    uid = uid.strip()
    uid = uid.replace("-", " ")
    uid = uid.replace(":", " ")
    uid = uid.replace(".", " ")
    uid = " ".join(uid.split())
    return uid.upper()


def ao_receber_mensagem(client, userdata, msg):
    # logger.debug(f"[MQTT] Topico: {msg.topic}, mensagem: {msg.payload.decode()}")

    global ultimo_uid_cadastro, ultimo_uid_presenca, ultimo_para_cadastro

    uid_recebido = msg.payload.decode().strip()
    uid_recebido = normalizar_uid(uid_recebido)

    print(f"[MQTT] Mensagem recebida no tópico {msg.topic}: '{uid_recebido}'")

    # Alterna entre cadastro e presença para preservar o fluxo atual do sistema.
    # Essa alternância mantém o comportamento que já existia no projeto.
    with estado_lock:
        destino = "cadastro" if ultimo_para_cadastro else "presenca"
        ultimo_para_cadastro = not ultimo_para_cadastro

        if destino == "cadastro":
            ultimo_uid_cadastro = uid_recebido
        else:
            ultimo_uid_presenca = uid_recebido

    # Adiciona cada UID à fila para processamento sequencial.
    fila_uids.put({
        "uid": uid_recebido,
        "destino": destino,
        "recebido_em": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    })

    print(f"[MQTT] UID enfileirado: '{uid_recebido}' - destino: {destino}")

def on_connect(client, userdata, flags, rc):
    """Callback executado quando o MQTT conectar com sucesso."""
    if rc == 0:
        print(f"[MQTT] Conectado ao broker {MQTT_BROKER} com sucesso")
        client.subscribe(MQTT_TOPIC)
        print(f"[MQTT] Inscrito no tópico: {MQTT_TOPIC}")
    else:
        print(f"[MQTT] Falha na conexão MQTT, código de retorno: {rc}")


def on_disconnect(client, userdata, rc):
    """Callback executado quando o MQTT se desconecta."""
    print(f"[MQTT] Desconectado do broker {MQTT_BROKER} com código: {rc}")


def iniciar_mqtt():
    cliente = mqtt.Client()
    cliente.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    cliente.on_connect = on_connect
    cliente.on_disconnect = on_disconnect
    cliente.on_message = ao_receber_mensagem

    try:
        resultado = cliente.connect(MQTT_BROKER, 1883)
        print(f"[MQTT] Tentando conectar ao broker {MQTT_BROKER}, retorno: {resultado}")
    except Exception as exc:
        print(f"[MQTT] Erro ao conectar no broker: {exc}")
        return

    cliente.loop_start()
    print("[MQTT] Loop de rede MQTT iniciado")

    # Mantém a thread viva enquanto o Flask roda.
    while True:
        try:
            if not cliente.is_connected():
                print("[MQTT] Conexão MQTT perdida, tentando reconectar...")
                cliente.reconnect()
            time.sleep(1)
        except Exception as exc:
            print(f"[MQTT] Erro no loop de reconexão: {exc}")
            time.sleep(5)


# ===== NOVA FUNCIONALIDADE: PROCESSADOR DE FILA =====

def registrar_evento_historico(
    uid: str,
    destino: str,
    situacao: str,
    id_aluno: str,
    nome: str,
    tipo: str,
) -> None:
    """Registra cada leitura no histórico para rastreabilidade."""
    print(f"[HISTORICO] UID='{uid}' destino='{destino}' situacao='{situacao}' id_aluno='{id_aluno}' nome='{nome}' tipo='{tipo}'")
    historico_leituras.append(
        {
            "uid": uid,
            "destino": destino,
            "situacao": situacao,
            "id_aluno": id_aluno,
            "nome": nome,
            "tipo": tipo,
            "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        }
    )


def processar_presenca(uid: str) -> None:
    """Processa automaticamente a presença quando o UID chega na fila."""
    agora = datetime.now()

    with estado_lock:
        ultimo_tempo = ultimas_leituras_por_uid.get(uid)
        print(f"[PRESENCA] Verificando duplicidade para UID='{uid}'")
        if ultimo_tempo and (agora - ultimo_tempo).total_seconds() < IGNORE_DUPLICATE_INTERVAL_SEGUNDOS:
            print(f"[PRESENCA] Ignorado por duplicidade: UID='{uid}' ultimo_tempo='{ultimo_tempo}' agora='{agora}'")
            registrar_evento_historico(
                uid,
                "presenca",
                "Ignorado por duplicidade",
                "-",
                "Cartão repetido",
                "ignorado",
            )
            return

        ultimas_leituras_por_uid[uid] = agora
        print(f"[PRESENCA] UID autorizado para processamento: '{uid}'")

    aluno = buscar_aluno_por_uid(uid)
    if aluno is not None:
        tipo = definir_tipo_registro(aluno["id_aluno"])
        novo_registro = {
            "id_aluno": aluno["id_aluno"],
            "nome": aluno["nome"],
            "uid": uid,
            "data_hora": agora.strftime("%d/%m/%Y %H:%M:%S"),
            "tipo": tipo,
        }
        registros.append(novo_registro)
        situacao = "Processado com sucesso"
        id_aluno = aluno["id_aluno"]
        nome = aluno["nome"]
    else:
        novo_registro = {
            "id_aluno": "-",
            "nome": "Cartão não identificado",
            "uid": uid,
            "data_hora": agora.strftime("%d/%m/%Y %H:%M:%S"),
            "tipo": "erro",
        }
        registros.append(novo_registro)
        situacao = "Aluno não encontrado"
        id_aluno = "-"
        nome = "Cartão não identificado"
        tipo = "erro"

    registrar_evento_historico(uid, "presenca", situacao, id_aluno, nome, tipo)

    with estado_lock:
        ultimo_evento_presenca["uid"] = uid
        ultimo_evento_presenca["id_aluno"] = id_aluno
        ultimo_evento_presenca["nome"] = nome
        ultimo_evento_presenca["data_hora"] = novo_registro["data_hora"]
        ultimo_evento_presenca["situacao"] = situacao
        ultimo_evento_presenca["tipo"] = tipo


def processar_fila() -> None:
    """Consume a fila de leituras MQTT e trata cada item de forma individual."""
    while True:
        try:
            item = fila_uids.get(timeout=1)
        except Empty:
            continue

        uid = item["uid"]
        destino = item["destino"]
        recebido_em = item["recebido_em"]
        print(f"[FILA] Desenfileirando UID='{uid}' destino='{destino}' recebido_em='{recebido_em}'")

        if destino == "cadastro":
            registrar_evento_historico(
                uid,
                destino,
                "Recebido para cadastro",
                "-",
                "Aguardando cadastro",
                "cadastro",
            )
        else:
            processar_presenca(uid)

        fila_uids.task_done()


# Base simulada de alunos
alunos: List[Dict[str, str]] = [
    {"id_aluno": "1", "nome": "Julia Augustinho", "id_nfc": "A8 10 15 C1 18"},
    {"id_aluno": "2", "nome": "Amanda", "id_nfc": "UID456"},
    {"id_aluno": "3", "nome": "Giovanna", "id_nfc": "UID789"},
]

# Registros de presença
registros: List[Dict[str, str]] = []


def buscar_aluno_por_uid(uid: str) -> Optional[Dict[str, str]]:
    """Busca um aluno pelo UID do cartão NFC."""
    uid = normalizar_uid(uid)
    print(f"[BUSCA_ALUNO] Iniciando busca para UID='{uid}'")
    for aluno in alunos:
        print(f"[BUSCA_ALUNO] Comparando com aluno id_aluno='{aluno['id_aluno']}' uid_salvo='{aluno['id_nfc']}'")
        if aluno["id_nfc"] == uid:
            print(f"[BUSCA_ALUNO] Aluno encontrado: id_aluno='{aluno['id_aluno']}' nome='{aluno['nome']}'")
            return aluno
    print(f"[BUSCA_ALUNO] Nenhum aluno encontrado para UID='{uid}'")
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

    with estado_lock:
        historico_recente = historico_leituras[-10:]

    return render_template(
        "presenca.html",
        registros=registros,
        lista_presenca=lista_presenca,
        total_presentes=presentes,
        ultimo_uid_presenca=ultimo_uid_presenca,
        historico_recente=historico_recente,
    )

@app.route("/status_presenca", methods=["GET"])
def status_presenca():
    """Retorna dados de presença para atualização automática da interface."""
    lista_presenca = calcular_lista_presenca()
    presentes = sum(1 for item in lista_presenca if item["status"] == "Presente")

    with estado_lock:
        historico_recente = historico_leituras[-10:]
        ultimo_presenca = ultimo_evento_presenca.copy()
        ultimo_uid = ultimo_uid_presenca
        fila_tamanho = fila_uids.qsize()

    return jsonify(
        {
            "status_sistema": "online",
            "ultimo_cartao_lido": ultimo_uid,
            "ultimo_aluno_identificado": ultimo_presenca.get("nome", "-"),
            "horario_ultima_leitura": ultimo_presenca.get("data_hora", "-"),
            "total_presentes": presentes,
            "lista_presenca": lista_presenca,
            "historico_recente": historico_recente,
            "fila_tamanho": fila_tamanho,
        }
    )

@app.route("/cadastrar_aluno", methods=["POST"])
def cadastrar_aluno():
    global ultimo_uid_cadastro
    
    id_aluno = request.form.get("id_aluno", "").strip()
    nome = request.form.get("nome", "").strip()
    id_nfc = request.form.get("id_nfc", "").strip()

    if id_aluno and nome and id_nfc:
        id_nfc = normalizar_uid(id_nfc)

        # Evita duplicidade de UID
        if not any(aluno["id_nfc"] == id_nfc for aluno in alunos):
            alunos.append(
                {
                    "id_aluno": id_aluno,
                    "nome": nome,
                    "id_nfc": id_nfc,
                }
            )
            print(f"[CADASTRO] Aluno cadastrado: id_aluno='{id_aluno}' nome='{nome}' id_nfc='{id_nfc}'")

            # ===== ALTERAÇÃO: REGISTRO AUTOMÁTICO DE PRESENÇA APÓS CADASTRO =====
            print(f"[CADASTRO] Registrando presença automaticamente para UID='{id_nfc}'")
            processar_presenca(id_nfc)

            # Limpa o UID após cadastro
            ultimo_uid_cadastro = ""
        else:
            print(f"[CADASTRO] UID já cadastrado: '{id_nfc}'")

    return redirect(url_for("cadastro_page"))


@app.route("/registrar_presenca", methods=["POST"])
def registrar_presenca():
    global ultimo_uid_presenca
    
    uid = request.form.get("uid", "").strip()
    uid = normalizar_uid(uid)

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

    # ===== NOVA FUNCIONALIDADE: PROCESSAMENTO ASSÍNCRONO DE FILA =====
    thread_fila = Thread(target=processar_fila, daemon=True)
    thread_fila.start()

    app.run(host="127.0.0.1", port=5000, debug=True, use_reloader=False)
    #iniciar_mqtt()    
   

# Inicia o MQTT em uma Thread separada para não travar o Flask
    # # thread_mqtt.daemon = True
    # thread_mqtt.start()