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


# ===== FUNÇÕES DE BANCO DE DADOS - ALUNOS =====

def obter_todos_alunos() -> List[Dict[str, str]]:
    """Obtém todos os alunos do banco e retorna como lista de dicts."""
    db = get_db_session()
    try:
        alunos = db.query(Aluno).all()
        return [aluno.to_dict() for aluno in alunos]
    finally:
        db.close()


def buscar_aluno_por_uid_db(uid: str) -> Optional[Aluno]:
    """Busca um aluno pelo UID no banco de dados."""
    uid = normalizar_uid(uid)
    db = get_db_session()
    try:
        print(f"[DB_BUSCA] Procurando UID='{uid}' no banco")
        aluno = db.query(Aluno).filter(Aluno.id_nfc == uid).first()
        if aluno:
            print(f"[DB_BUSCA] Aluno encontrado: id_aluno='{aluno.id_aluno}' nome='{aluno.nome}'")
            # Desanexa o objeto da sessão para evitar problemas
            db.expunge(aluno)
            return aluno
        else:
            print(f"[DB_BUSCA] Nenhum aluno encontrado para UID='{uid}'")
            return None
    finally:
        db.close()


def criar_aluno(ra: str, nome: str, id_nfc: str) -> bool:
    """
    Cria um novo aluno no banco (FASE 4: usa ra em vez de id_aluno).
    Retorna True se bem-sucedido, False se UID já existe.
    """
    id_nfc = normalizar_uid(id_nfc)
    db = get_db_session()
    try:
        # Verifica se UID já existe
        aluno_existente = db.query(Aluno).filter(Aluno.id_nfc == id_nfc).first()
        if aluno_existente:
            print(f"[DB_CRIAR] UID já cadastrado: '{id_nfc}'")
            return False

        novo_aluno = Aluno(ra=ra, nome=nome, id_nfc=id_nfc)
        db.add(novo_aluno)
        db.commit()
        print(f"[DB_CRIAR] Aluno cadastrado: RA='{ra}' nome='{nome}' id_nfc='{id_nfc}'")
        return True
    except Exception as e:
        print(f"[DB_CRIAR] Erro ao criar aluno: {e}")
        db.rollback()
        return False
    finally:
        db.close()


# ===== FUNÇÕES DE BANCO DE DADOS - PRESENÇA =====

def buscar_ultima_presenca_db(ra: str) -> Optional[Presenca]:
    """Busca a última presença registrada de um aluno pelo RA."""
    db = get_db_session()
    try:
        print(f"[DB_PRESENCA] Procurando última presença para RA='{ra}'")
        ultima_presenca = (
            db.query(Presenca)
            .filter(Presenca.ra == ra)
            .order_by(Presenca.criado_em.desc())
            .first()
        )
        if ultima_presenca:
            print(f"[DB_PRESENCA] Encontrada: tipo='{ultima_presenca.tipo}' data_hora='{ultima_presenca.data_hora}'")
            db.expunge(ultima_presenca)
            return ultima_presenca
        else:
            print(f"[DB_PRESENCA] Nenhuma presença anterior encontrada para RA='{ra}'")
            return None
    finally:
        db.close()


def verificar_anti_duplicidade_40min(ra: str) -> tuple[bool, str]:
    """
    Verifica se pode registrar uma nova presença considerando janela de 40 minutos.
    
    Retorna:
        (pode_registrar: bool, motivo: str)
        - (True, "OK") - pode registrar
        - (False, "motivo") - não pode registrar
    """
    ultima_presenca = buscar_ultima_presenca_db(ra)
    
    if not ultima_presenca:
        print(f"[ANTI_DUP] RA='{ra}': primeira presença, autorizado")
        return True, "Primeira presença"
    
    # Parse data_hora do formato "DD/MM/YYYY HH:MM:SS"
    try:
        ultima_data = datetime.strptime(ultima_presenca.data_hora, "%d/%m/%Y %H:%M:%S")
        agora = datetime.now()
        diferenca_minutos = (agora - ultima_data).total_seconds() / 60
        
        print(f"[ANTI_DUP] RA='{ra}': última presença há {diferenca_minutos:.1f} minutos")
        
        if diferenca_minutos < 40:
            motivo = f"Presença já registrada há {int(diferenca_minutos)} minutos. Aguarde 40 minutos."
            print(f"[ANTI_DUP] RA='{ra}': {motivo}")
            return False, motivo
        else:
            print(f"[ANTI_DUP] RA='{ra}': janela de 40 min atendida, autorizado")
            return True, "Autorizado após 40 minutos"
    except Exception as e:
        print(f"[ANTI_DUP] Erro ao processar data: {e}")
        return True, "Erro em verificação (autorizado por segurança)"


def registrar_presenca_bd(ra: str, tipo: str, data_hora: str) -> bool:
    """
    Registra uma nova presença no banco de dados.
    
    Args:
        ra: RA do aluno
        tipo: "entrada", "saida", "erro", "ignorado"
        data_hora: string no formato "DD/MM/YYYY HH:MM:SS"
    
    Returns:
        True se registrado com sucesso, False caso contrário
    """
    db = get_db_session()
    try:
        nova_presenca = Presenca(ra=ra, tipo=tipo, data_hora=data_hora)
        db.add(nova_presenca)
        db.commit()
        print(f"[DB_PRESENCA_REG] Presença registrada: ra='{ra}' tipo='{tipo}' data_hora='{data_hora}'")
        return True
    except Exception as e:
        print(f"[DB_PRESENCA_REG] Erro ao registrar presença: {e}")
        db.rollback()
        return False
    finally:
        db.close()


def obter_presencas_db(ra: str = None, limite: int = None) -> List[Dict]:
    """
    Obtém registros de presença do banco.
    
    Args:
        ra: Filtro opcional por RA. Se None, retorna todos.
        limite: Limita quantidade de resultados. Se None, sem limite.
    
    Returns:
        Lista de dicts com informações de presença
    """
    db = get_db_session()
    try:
        query = db.query(Presenca).order_by(Presenca.criado_em.desc())
        
        if ra:
            query = query.filter(Presenca.ra == ra)
        
        if limite:
            query = query.limit(limite)
        
        presencas = query.all()
        resultado = [p.to_dict() for p in presencas]
        
        print(f"[DB_PRESENCA_OBTER] Retornando {len(resultado)} presencas")
        return resultado
    finally:
        db.close()


# ===== NOVA FUNCIONALIDADE: FILA DE PROCESSAMENTO NFC =====
fila_uids: Queue[Dict[str, str]] = Queue()
historico_leituras: List[Dict[str, str]] = []
ultimo_evento_presenca: Dict[str, str] = {
    "uid": "",
    "ra": "",
    "nome": "",
    "data_hora": "",
    "situacao": "",
    "tipo": "",
}
ultimo_uid_cadastro = ""
ultimo_uid_presenca = ""
estado_lock = Lock()


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

    global ultimo_uid_cadastro, ultimo_uid_presenca

    uid_recebido = msg.payload.decode().strip()
    uid_recebido = normalizar_uid(uid_recebido)

    print(f"[MQTT] Mensagem recebida no tópico {msg.topic}: '{uid_recebido}'")

    # ===== FASE 3: RECONHECIMENTO AUTOMÁTICO POR UID =====
    # Consulta banco para determinar se é cadastro ou presença
    aluno_encontrado = buscar_aluno_por_uid_db(uid_recebido)
    
    with estado_lock:
        if aluno_encontrado:
            destino = "presenca"
            ultimo_uid_presenca = uid_recebido
            print(f"[MQTT] UID reconhecido: RA='{aluno_encontrado.ra}' nome='{aluno_encontrado.nome}'")
        else:
            destino = "cadastro"
            ultimo_uid_cadastro = uid_recebido
            print(f"[MQTT] UID desconhecido: encaminhando para cadastro")

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


# ===== PROCESSADOR DE FILA =====

def registrar_evento_historico(
    uid: str,
    ra: str,
    nome: str,
    destino: str,
    situacao: str,
    tipo: str,
) -> None:
    """Registra cada leitura no histórico para rastreabilidade (FASE 4: com RA em vez de ID)."""
    print(f"[HISTORICO] UID='{uid}' RA='{ra}' nome='{nome}' destino='{destino}' situacao='{situacao}' tipo='{tipo}'")
    historico_leituras.append(
        {
            "uid": uid,
            "ra": ra,
            "nome": nome,
            "destino": destino,
            "situacao": situacao,
            "tipo": tipo,
            "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        }
    )


def processar_presenca(uid: str) -> None:
    """
    FASE 4: Processa automaticamente a presença com anti-duplicidade de 40 minutos.
    
    Fluxo:
    1. Busca aluno pelo UID
    2. Se não encontrado: registra como erro
    3. Se encontrado:
       a. Verifica janela anti-duplicidade de 40 minutos
       b. Se dentro da janela: ignora (presença já registrada)
       c. Se fora da janela: define tipo (entrada/saída) e registra em BD
    """
    agora = datetime.now()
    agora_formatada = agora.strftime("%d/%m/%Y %H:%M:%S")
    
    print(f"[PRESENCA] Processando UID='{uid}'")
    
    # Busca aluno no banco
    aluno = buscar_aluno_por_uid_db(uid)
    
    if aluno is None:
        print(f"[PRESENCA] Aluno não encontrado para UID='{uid}'")
        registrar_evento_historico(
            uid=uid,
            ra="-",
            nome="Cartão não identificado",
            destino="presenca",
            situacao="Aluno não encontrado",
            tipo="erro",
        )
        with estado_lock:
            ultimo_evento_presenca["uid"] = uid
            ultimo_evento_presenca["ra"] = "-"
            ultimo_evento_presenca["nome"] = "Cartão não identificado"
            ultimo_evento_presenca["data_hora"] = agora_formatada
            ultimo_evento_presenca["situacao"] = "Aluno não encontrado"
            ultimo_evento_presenca["tipo"] = "erro"
        return
    
    # Aluno encontrado: verifica anti-duplicidade
    ra = aluno.ra
    pode_registrar, motivo = verificar_anti_duplicidade_40min(ra)
    
    if not pode_registrar:
        print(f"[PRESENCA] RA='{ra}': {motivo}")
        registrar_evento_historico(
            uid=uid,
            ra=ra,
            nome=aluno.nome,
            destino="presenca",
            situacao=motivo,
            tipo="ignorado",
        )
        with estado_lock:
            ultimo_evento_presenca["uid"] = uid
            ultimo_evento_presenca["ra"] = ra
            ultimo_evento_presenca["nome"] = aluno.nome
            ultimo_evento_presenca["data_hora"] = agora_formatada
            ultimo_evento_presenca["situacao"] = motivo
            ultimo_evento_presenca["tipo"] = "ignorado"
        return
    
    # Pode registrar: define tipo e registra no BD
    tipo = definir_tipo_registro(ra)
    sucesso = registrar_presenca_bd(ra=ra, tipo=tipo, data_hora=agora_formatada)
    
    if sucesso:
        situacao = f"Presença registrada como {tipo}"
        print(f"[PRESENCA] RA='{ra}': presença registrada ({tipo})")
    else:
        situacao = "Erro ao registrar no banco"
        tipo = "erro"
        print(f"[PRESENCA] RA='{ra}': erro ao registrar")
    
    registrar_evento_historico(
        uid=uid,
        ra=ra,
        nome=aluno.nome,
        destino="presenca",
        situacao=situacao,
        tipo=tipo,
    )
    
    with estado_lock:
        ultimo_evento_presenca["uid"] = uid
        ultimo_evento_presenca["ra"] = ra
        ultimo_evento_presenca["nome"] = aluno.nome
        ultimo_evento_presenca["data_hora"] = agora_formatada
        ultimo_evento_presenca["situacao"] = situacao
        ultimo_evento_presenca["tipo"] = tipo


def processar_fila() -> None:
    """
    Consumer da fila MQTT.
    
    FASE 3: Reconhecimento automático por UID
    - Se destino="presenca": UID foi reconhecido no banco → registra entrada/saída
    - Se destino="cadastro": UID é novo → aguarda cadastro do usuário
    """
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
            # UID desconhecido: aguarda cadastro
            registrar_evento_historico(
                uid=uid,
                ra="-",
                nome="Aguardando cadastro",
                destino="cadastro",
                situacao="Recebido para cadastro",
                tipo="cadastro",
            )
        else:
            # UID reconhecido: processa presença automaticamente
            processar_presenca(uid)

        fila_uids.task_done()


# ===== FUNÇÕES DE SUPORTE =====

def buscar_aluno_por_uid(uid: str) -> Optional[Dict[str, str]]:
    """
    Busca um aluno pelo UID do cartão NFC no banco de dados.
    Retorna um dicionário compatível com o código existente.
    """
    aluno = buscar_aluno_por_uid_db(uid)
    if aluno:
        return aluno.to_dict()
    return None


def definir_tipo_registro(ra: str) -> str:
    """
    FASE 4: Define se o próximo registro será entrada ou saída consultando o BD.
    
    Regra:
    - Se não houver registro anterior: entrada (primeira presença)
    - Se último registro foi entrada: saída
    - Caso contrário: entrada
    """
    ultima_presenca = buscar_ultima_presenca_db(ra)
    
    if not ultima_presenca:
        print(f"[TIPO] RA='{ra}': primeira presença, tipo=entrada")
        return "entrada"
    
    tipo_resultado = "saida" if ultima_presenca.tipo == "entrada" else "entrada"
    print(f"[TIPO] RA='{ra}': último={ultima_presenca.tipo}, próximo={tipo_resultado}")
    return tipo_resultado


def calcular_lista_presenca() -> List[Dict[str, str]]:
    """
    FASE 4: Retorna a lista de presença atual de cada aluno consultando o BD.
    
    Para cada aluno:
    - Busca última presença no BD
    - Se último tipo foi entrada: Presente
    - Se último tipo foi saída: Ausente
    - Se não há presença: Ausente
    """
    lista_presenca: List[Dict[str, str]] = []
    alunos = obter_todos_alunos()

    for aluno in alunos:
        ra = aluno["ra"]
        ultima_presenca = buscar_ultima_presenca_db(ra)
        
        if ultima_presenca and ultima_presenca.tipo in {"entrada", "saida"}:
            status = "Presente" if ultima_presenca.tipo == "entrada" else "Ausente"
            ultima_hora = ultima_presenca.data_hora
        else:
            status = "Ausente"
            ultima_hora = "-"

        lista_presenca.append(
            {
                "ra": aluno["ra"],
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
    presencas_recentes = obter_presencas_db(limite=20)

    return render_template(
        "index.html",
        alunos=obter_todos_alunos(),
        registros=presencas_recentes,
        lista_presenca=lista_presenca,
        total_presentes=presentes,
    )

@app.route("/cadastro", methods=["GET"])
def cadastro_page():
    return render_template(
        "cadastro.html",
        alunos=obter_todos_alunos(),
        ultimo_uid_cadastro=ultimo_uid_cadastro,
    )

@app.route("/presenca", methods=["GET"])
def presenca_page():
    lista_presenca = calcular_lista_presenca()
    presentes = sum(1 for item in lista_presenca if item["status"] == "Presente")
    presencas_recentes = obter_presencas_db(limite=50)

    with estado_lock:
        historico_recente = historico_leituras[-10:]

    return render_template(
        "presenca.html",
        registros=presencas_recentes,
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
    
    ra = request.form.get("id_aluno", "").strip()  # HTML form ainda usa id_aluno
    nome = request.form.get("nome", "").strip()
    id_nfc = request.form.get("id_nfc", "").strip()

    if ra and nome and id_nfc:
        # Tenta criar o aluno no banco
        sucesso = criar_aluno(ra, nome, id_nfc)
        
        if sucesso:
            # ===== FASE 4: REGISTRO AUTOMÁTICO DE PRESENÇA APÓS CADASTRO =====
            print(f"[CADASTRO] Registrando presença automaticamente para UID='{id_nfc}'")
            processar_presenca(id_nfc)

            # Limpa o UID após cadastro
            ultimo_uid_cadastro = ""
        else:
            print(f"[CADASTRO] Falha ao cadastrar aluno (UID pode estar duplicado)")

    return redirect(url_for("cadastro_page"))


@app.route("/registrar_presenca", methods=["POST"])
def registrar_presenca():
    """
    FASE 4: Rota manual para registrar presença.
    Agora usa processar_presenca que verifica anti-duplicidade e registra em BD.
    """
    global ultimo_uid_presenca
    
    uid = request.form.get("uid", "").strip()
    uid = normalizar_uid(uid)

    if not uid:
        return redirect(url_for("presenca_page"))

    # Processa presença através do pipeline normal (anti-duplicidade + BD)
    processar_presenca(uid)
    
    # Limpa o UID após registrar
    ultimo_uid_presenca = ""

    return redirect(url_for("presenca_page"))


@app.route("/limpar_registros", methods=["POST"])
def limpar_registros():
    """
    FASE 4: Limpa todos os registros de presença do banco de dados.
    CUIDADO: Esta operação é irreversível!
    """
    db = get_db_session()
    try:
        db.query(Presenca).delete()
        db.commit()
        print("[DB_LIMPAR] Todos os registros de presença foram removidos")
    except Exception as e:
        print(f"[DB_LIMPAR] Erro ao limpar registros: {e}")
        db.rollback()
    finally:
        db.close()
    
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