from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
import io

from flask import Flask, Response, redirect, render_template, request, url_for, jsonify
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

import paho.mqtt.client as mqtt
import time
from queue import Queue, Empty
from threading import Thread, Lock

from models import Base, Aluno, Presenca, SessaoChamada

app = Flask(__name__)

# ===== CONFIGURAÇÃO DO BANCO DE DADOS =====
DATABASE_URL = "sqlite:///presenca.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Cria as tabelas automaticamente se não existirem e aplica migrações necessárias

def executar_migracoes() -> None:
    """Cria tabelas novas e atualiza a estrutura existente do banco."""
    Base.metadata.create_all(bind=engine)

    with engine.begin() as conn:
        resultado = conn.execute(text("PRAGMA table_info(presencas)")).fetchall()
        colunas = [linha[1] for linha in resultado]
        if "sessao_id" not in colunas:
            conn.execute(text("ALTER TABLE presencas ADD COLUMN sessao_id INTEGER"))
            print("[DB_MIGRACAO] Coluna sessao_id adicionada em presencas")

    with engine.begin() as conn:
        resultado = conn.execute(text("PRAGMA table_info(sessoes_chamada)")).fetchall()
        colunas = [linha[1] for linha in resultado]
        if "disciplina" not in colunas:
            conn.execute(text("ALTER TABLE sessoes_chamada ADD COLUMN disciplina VARCHAR(200) NOT NULL DEFAULT ''"))
            print("[DB_MIGRACAO] Coluna disciplina adicionada em sessoes_chamada")
        if "turma" not in colunas:
            conn.execute(text("ALTER TABLE sessoes_chamada ADD COLUMN turma VARCHAR(100) NOT NULL DEFAULT ''"))
            print("[DB_MIGRACAO] Coluna turma adicionada em sessoes_chamada")
        if "professor" not in colunas:
            conn.execute(text("ALTER TABLE sessoes_chamada ADD COLUMN professor VARCHAR(200) NOT NULL DEFAULT ''"))
            print("[DB_MIGRACAO] Coluna professor adicionada em sessoes_chamada")

executar_migracoes()
print("[DB] Tabelas criadas/verificadas com sucesso")


def get_db_session() -> Session:
    """Retorna uma nova sessão de banco de dados."""
    return SessionLocal()


def obter_sessao_aberta_db() -> Optional[SessaoChamada]:
    """Retorna a sessão de chamada escolar que estiver aberta, se existir."""
    db = get_db_session()
    try:
        sessao = (
            db.query(SessaoChamada)
            .filter(SessaoChamada.status == "ABERTA")
            .order_by(SessaoChamada.id.desc())
            .first()
        )
        if sessao:
            db.expunge(sessao)
        return sessao
    finally:
        db.close()


def abrir_chamada_db(disciplina: str, turma: str, professor: str) -> SessaoChamada:
    """Cria uma nova sessão de chamada aberta."""
    db = get_db_session()
    try:
        agora = datetime.now()
        sessao = SessaoChamada(
            data=agora.strftime("%d/%m/%Y"),
            hora_inicio=agora.strftime("%H:%M:%S"),
            status="ABERTA",
            disciplina=disciplina,
            turma=turma,
            professor=professor,
        )
        db.add(sessao)
        db.commit()
        db.refresh(sessao)
        print(
            f"[CHAMADA] Sessão de chamada aberta: id={sessao.id} data={sessao.data} "
            f"hora_inicio={sessao.hora_inicio} disciplina={sessao.disciplina} turma={sessao.turma} "
            f"professor={sessao.professor}"
        )
        return sessao
    except Exception as e:
        db.rollback()
        print(f"[CHAMADA] Erro ao abrir chamada: {e}")
        raise
    finally:
        db.close()


def encerrar_chamada_db() -> Optional[SessaoChamada]:
    """Encerra a sessão de chamada atualmente aberta."""
    db = get_db_session()
    try:
        sessao = (
            db.query(SessaoChamada)
            .filter(SessaoChamada.status == "ABERTA")
            .order_by(SessaoChamada.id.desc())
            .first()
        )
        if not sessao:
            print("[CHAMADA] Nenhuma chamada aberta para encerrar")
            return None

        agora = datetime.now()
        sessao.status = "ENCERRADA"
        sessao.hora_fim = agora.strftime("%H:%M:%S")
        db.commit()
        print(f"[CHAMADA] Sessão de chamada encerrada: id={sessao.id}")
        db.expunge(sessao)
        return sessao
    except Exception as e:
        db.rollback()
        print(f"[CHAMADA] Erro ao encerrar chamada: {e}")
        return None
    finally:
        db.close()


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
            print(f"[DB_BUSCA] Aluno encontrado: RA='{aluno.ra}' nome='{aluno.nome}'")
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

def presenca_existe_na_sessao(ra: str, sessao_id: int) -> bool:
    """Verifica se o aluno já registrou presença na sessão de chamada atual."""
    db = get_db_session()
    try:
        return (
            db.query(Presenca)
            .filter(Presenca.ra == ra, Presenca.sessao_id == sessao_id)
            .first()
            is not None
        )
    finally:
        db.close()


def obter_hora_presenca_na_sessao(ra: str, sessao_id: int) -> str:
    db = get_db_session()
    try:
        presenca = (
            db.query(Presenca)
            .filter(Presenca.ra == ra, Presenca.sessao_id == sessao_id)
            .order_by(Presenca.criado_em.desc())
            .first()
        )
        return presenca.data_hora if presenca else "-"
    finally:
        db.close()


def registrar_presenca_bd(ra: str, sessao_id: int, data_hora: str, tipo: str = "presenca") -> bool:
    """
    Registra uma nova presença no banco de dados.
    """
    db = get_db_session()
    try:
        nova_presenca = Presenca(
            ra=ra,
            sessao_id=sessao_id,
            data_hora=data_hora,
            tipo=tipo,
        )
        db.add(nova_presenca)
        db.commit()
        print(f"[DB_PRESENCA_REG] Presença registrada: ra='{ra}' sessao_id={sessao_id} data_hora='{data_hora}'")
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


def obter_sessao_por_id(sessao_id: int) -> Optional[SessaoChamada]:
    db = get_db_session()
    try:
        sessao = db.query(SessaoChamada).filter(SessaoChamada.id == sessao_id).first()
        if sessao:
            db.expunge(sessao)
        return sessao
    finally:
        db.close()


def obter_sessoes_encerradas_db(limite: int = None) -> List[Dict[str, str]]:
    db = get_db_session()
    try:
        query = db.query(SessaoChamada).filter(SessaoChamada.status == "ENCERRADA").order_by(SessaoChamada.id.desc())
        if limite:
            query = query.limit(limite)
        sessoes = query.all()
        resultado = [sessao.to_dict() for sessao in sessoes]
        print(f"[DB_CHAMADA_OBTER] Retornando {len(resultado)} sessoes encerradas")
        return resultado
    finally:
        db.close()


def obter_presencas_da_sessao(sessao_id: int) -> List[Dict[str, str]]:
    db = get_db_session()
    try:
        presencas = (
            db.query(Presenca)
            .filter(Presenca.sessao_id == sessao_id)
            .order_by(Presenca.criado_em.asc())
            .all()
        )
        resultado = [p.to_dict() for p in presencas]
        print(f"[DB_CHAMADA_PRESENCA] Retornando {len(resultado)} presencas para sessao_id={sessao_id}")
        return resultado
    finally:
        db.close()


def obter_ultima_sessao_encerrada_db() -> Optional[SessaoChamada]:
    db = get_db_session()
    try:
        sessao = (
            db.query(SessaoChamada)
            .filter(SessaoChamada.status == "ENCERRADA")
            .order_by(SessaoChamada.id.desc())
            .first()
        )
        if sessao:
            db.expunge(sessao)
        return sessao
    finally:
        db.close()


def criar_texto_presenca(sessao: SessaoChamada, presencas: List[Dict[str, str]]) -> bytes:
    cabecalho = [
        "FATEC IPIRANGA",
        "Lista de Presença",
        f"Disciplina: {sessao.disciplina}",
        f"Turma: {sessao.turma}",
        f"Professor: {sessao.professor}",
        f"Data: {sessao.data}",
        f"Início: {sessao.hora_inicio}",
        f"Fim: {sessao.hora_fim or '-'}",
        "",
        "RA | Nome | Horário",
        "----------------------------------------",
    ]
    linhas = cabecalho[:]
    for presenca in presencas:
        linhas.append(f"{presenca['ra']} | {presenca['nome']} | {presenca['data_hora']}")
    texto = "\n".join(linhas) + "\n"
    return texto.encode("utf-8")


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def criar_pdf_presenca(sessao: SessaoChamada, presencas: List[Dict[str, str]]) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    except Exception:
        # Se ReportLab não estiver instalado, retorna o TXT como fallback
        return criar_texto_presenca(sessao, presencas)

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=40, leftMargin=40, topMargin=60, bottomMargin=40)
    styles = getSampleStyleSheet()

    elementos = []
    elementos.append(Paragraph("FATEC IPIRANGA", styles["Title"]))
    elementos.append(Paragraph("Lista de Presença", styles["Heading2"]))
    elementos.append(Spacer(1, 12))

    info_text = (
        f"Disciplina: {sessao.disciplina} | Turma: {sessao.turma} | Professor: {sessao.professor}"
        f"<br/>Data: {sessao.data} | Início: {sessao.hora_inicio} | Fim: {sessao.hora_fim or '-'}"
    )
    elementos.append(Paragraph(info_text, styles["Normal"]))
    elementos.append(Spacer(1, 12))

    tabela_dados = [["RA", "Nome", "Horário"]]
    for p in presencas:
        tabela_dados.append([p.get("ra", "-"), p.get("nome", "-"), p.get("data_hora", "-")])

    tabela = Table(tabela_dados, colWidths=[80, 320, 120])
    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
    ]))

    elementos.append(tabela)

    doc.build(elementos)
    return buffer.getvalue()


def exportar_presenca_por_sessao(sessao_id: int, formato: str = "pdf") -> Response:
    sessao = obter_sessao_por_id(sessao_id)
    if not sessao:
        return redirect(url_for("historico_page"))

    presencas = obter_presencas_da_sessao(sessao_id)
    if formato == "txt":
        data_bytes = criar_texto_presenca(sessao, presencas)
        response = Response(data_bytes, mimetype="text/plain; charset=utf-8")
        nome_arquivo = f"lista_presenca_{sessao.data.replace('/', '-')}_{sessao.id}.txt"
    else:
        data_bytes = criar_pdf_presenca(sessao, presencas)
        response = Response(data_bytes, mimetype="application/pdf")
        nome_arquivo = f"lista_presenca_{sessao.data.replace('/', '-')}_{sessao.id}.pdf"

    response.headers["Content-Disposition"] = f"attachment; filename={nome_arquivo}"
    return response


def atualizar_aluno(ra: str, nome: str, id_nfc: str) -> bool:
    db = get_db_session()
    try:
        aluno = db.query(Aluno).filter(Aluno.ra == ra).first()
        if not aluno:
            return False
        aluno.nome = nome
        aluno.id_nfc = normalizar_uid(id_nfc)
        db.commit()
        print(f"[DB_ALUNO] Aluno atualizado: RA='{ra}' nome='{nome}' id_nfc='{id_nfc}'")
        return True
    except Exception as e:
        db.rollback()
        print(f"[DB_ALUNO] Erro ao atualizar aluno: {e}")
        return False
    finally:
        db.close()


def excluir_aluno_db(ra: str) -> bool:
    db = get_db_session()
    try:
        aluno = db.query(Aluno).filter(Aluno.ra == ra).first()
        if not aluno:
            return False
        db.delete(aluno)
        db.commit()
        print(f"[DB_ALUNO] Aluno excluído: RA='{ra}'")
        return True
    except Exception as e:
        db.rollback()
        print(f"[DB_ALUNO] Erro ao excluir aluno: {e}")
        return False
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
ultima_sessao_encerrada_id: Optional[int] = None
estado_lock = Lock()


# ===== CONFIGURAÇÃO MQTT =====
MQTT_BROKER = "brw.net.br"
MQTT_TOPIC = "aluno/id"
MQTT_USERNAME = "brware"
MQTT_PASSWORD = "SQRT(pi)!=314"

# ===== NORMALIZAÇÃO DE UID NFC =====
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
    global ultimo_uid_cadastro, ultimo_uid_presenca

    uid_recebido = msg.payload.decode().strip()
    uid_recebido = normalizar_uid(uid_recebido)

    print(f"[MQTT] Mensagem recebida no tópico {msg.topic}: '{uid_recebido}'")

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
    """Processa a presença de um aluno em uma chamada escolar."""
    agora = datetime.now()
    agora_formatada = agora.strftime("%d/%m/%Y %H:%M:%S")

    print(f"[PRESENCA] Processando UID='{uid}'")

    sessao = obter_sessao_aberta_db()
    if not sessao:
        situacao = "Não existe chamada aberta."
        print(f"[PRESENCA] UID='{uid}': {situacao}")
        registrar_evento_historico(
            uid=uid,
            ra="-",
            nome="Sem chamada aberta",
            destino="presenca",
            situacao=situacao,
            tipo="ignorado",
        )
        with estado_lock:
            ultimo_evento_presenca["uid"] = uid
            ultimo_evento_presenca["ra"] = "-"
            ultimo_evento_presenca["nome"] = "Sem chamada aberta"
            ultimo_evento_presenca["data_hora"] = agora_formatada
            ultimo_evento_presenca["situacao"] = situacao
            ultimo_evento_presenca["tipo"] = "ignorado"
        return

    aluno = buscar_aluno_por_uid_db(uid)
    if aluno is None:
        situacao = "Aluno não encontrado"
        print(f"[PRESENCA] UID='{uid}': {situacao}")
        registrar_evento_historico(
            uid=uid,
            ra="-",
            nome="Cartão não identificado",
            destino="presenca",
            situacao=situacao,
            tipo="erro",
        )
        with estado_lock:
            ultimo_evento_presenca["uid"] = uid
            ultimo_evento_presenca["ra"] = "-"
            ultimo_evento_presenca["nome"] = "Cartão não identificado"
            ultimo_evento_presenca["data_hora"] = agora_formatada
            ultimo_evento_presenca["situacao"] = situacao
            ultimo_evento_presenca["tipo"] = "erro"
        return

    ra = aluno.ra
    if presenca_existe_na_sessao(ra, sessao.id):
        situacao = "Presença já registrada nesta chamada."
        print(f"[PRESENCA] RA='{ra}': {situacao}")
        registrar_evento_historico(
            uid=uid,
            ra=ra,
            nome=aluno.nome,
            destino="presenca",
            situacao=situacao,
            tipo="ignorado",
        )
        with estado_lock:
            ultimo_evento_presenca["uid"] = uid
            ultimo_evento_presenca["ra"] = ra
            ultimo_evento_presenca["nome"] = aluno.nome
            ultimo_evento_presenca["data_hora"] = agora_formatada
            ultimo_evento_presenca["situacao"] = situacao
            ultimo_evento_presenca["tipo"] = "ignorado"
        return

    sucesso = registrar_presenca_bd(
        ra=ra,
        sessao_id=sessao.id,
        data_hora=agora_formatada,
        tipo="presenca",
    )

    if sucesso:
        situacao = "Presença registrada nesta chamada."
        print(f"[PRESENCA] RA='{ra}': presença registrada na chamada")
    else:
        situacao = "Erro ao registrar no banco"
        print(f"[PRESENCA] RA='{ra}': erro ao registrar")

    registrar_evento_historico(
        uid=uid,
        ra=ra,
        nome=aluno.nome,
        destino="presenca",
        situacao=situacao,
        tipo="presenca" if sucesso else "erro",
    )

    with estado_lock:
        ultimo_evento_presenca["uid"] = uid
        ultimo_evento_presenca["ra"] = ra
        ultimo_evento_presenca["nome"] = aluno.nome
        ultimo_evento_presenca["data_hora"] = agora_formatada
        ultimo_evento_presenca["situacao"] = situacao
        ultimo_evento_presenca["tipo"] = "presenca" if sucesso else "erro"


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

def calcular_lista_presenca() -> List[Dict[str, str]]:
    """Retorna a lista de presença atual dos alunos na sessão de chamada aberta."""
    lista_presenca: List[Dict[str, str]] = []
    sessao = obter_sessao_aberta_db()
    sessao_id = sessao.id if sessao else None

    for aluno in obter_todos_alunos():
        presente = sessao_id is not None and presenca_existe_na_sessao(aluno["ra"], sessao_id)
        status = "Presente" if presente else "Ausente"
        ultima_hora = (
            obter_hora_presenca_na_sessao(aluno["ra"], sessao_id)
            if presente
            else "-"
        )

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
    sessao_aberta = obter_sessao_aberta_db()
    sessoes_encerradas = obter_sessoes_encerradas_db(limite=5)

    return render_template(
        "index.html",
        sessao_aberta=sessao_aberta,
        sessoes_encerradas=sessoes_encerradas,
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
    sessao_aberta = obter_sessao_aberta_db()
    lista_presenca = calcular_lista_presenca()
    presentes = sum(1 for item in lista_presenca if item["status"] == "Presente")
    presencas_recentes = obter_presencas_db(limite=50)
    presentes_lista = [item for item in lista_presenca if item["status"] == "Presente"]

    with estado_lock:
        historico_recente = historico_leituras[-10:]
        ultimo_evento = ultimo_evento_presenca.copy()

    ultima_sessao_encerrada = None
    if not sessao_aberta and ultima_sessao_encerrada_id:
        ultima_sessao_encerrada = obter_ultima_sessao_encerrada_db()

    return render_template(
        "presenca.html",
        registros=presencas_recentes,
        lista_presenca=lista_presenca,
        presentes_lista=presentes_lista,
        total_presentes=presentes,
        ultimo_uid_presenca=ultimo_uid_presenca,
        ultimo_evento_presenca=ultimo_evento,
        historico_recente=historico_recente,
        sessao_aberta=sessao_aberta is not None,
        sessao=sessao_aberta,
        ultima_sessao_encerrada=ultima_sessao_encerrada,
        status_chamada=sessao_aberta.status if sessao_aberta else "Nenhuma chamada aberta",
        hora_inicio=sessao_aberta.hora_inicio if sessao_aberta else "-",
        hora_fim=sessao_aberta.hora_fim if sessao_aberta else "-",
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

    sessao_aberta = obter_sessao_aberta_db()

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
            "sessao_aberta": sessao_aberta is not None,
            "status_chamada": sessao_aberta.status if sessao_aberta else "Nenhuma chamada aberta",
            "hora_inicio": sessao_aberta.hora_inicio if sessao_aberta else "-",
            "hora_fim": sessao_aberta.hora_fim if sessao_aberta else "-",
            "disciplina": sessao_aberta.disciplina if sessao_aberta else "-",
            "turma": sessao_aberta.turma if sessao_aberta else "-",
            "professor": sessao_aberta.professor if sessao_aberta else "-",
        }
    )


@app.route("/abrir_chamada", methods=["POST"])
def abrir_chamada():
    global ultima_sessao_encerrada_id
    if not obter_sessao_aberta_db():
        disciplina = request.form.get("disciplina", "").strip()
        turma = request.form.get("turma", "").strip()
        professor = request.form.get("professor", "").strip()
        if disciplina and turma and professor:
            abrir_chamada_db(disciplina=disciplina, turma=turma, professor=professor)
            ultima_sessao_encerrada_id = None
    return redirect(url_for("presenca_page"))


@app.route("/encerrar_chamada", methods=["POST"])
def encerrar_chamada():
    global ultima_sessao_encerrada_id
    sessao = encerrar_chamada_db()
    if sessao:
        ultima_sessao_encerrada_id = sessao.id
    return redirect(url_for("presenca_page"))


@app.route("/alunos", methods=["GET"])
def alunos_page():
    return render_template(
        "alunos.html",
        alunos=obter_todos_alunos(),
    )


@app.route("/editar_aluno/<string:ra>", methods=["GET", "POST"])
def editar_aluno(ra: str):
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        id_nfc = request.form.get("id_nfc", "").strip()
        if nome and id_nfc:
            atualizar_aluno(ra, nome, id_nfc)
        return redirect(url_for("alunos_page"))

    db = get_db_session()
    try:
        aluno = db.query(Aluno).filter(Aluno.ra == ra).first()
        if not aluno:
            return redirect(url_for("alunos_page"))
        return render_template("editar_aluno.html", aluno=aluno)
    finally:
        db.close()


@app.route("/excluir_aluno/<string:ra>", methods=["POST"])
def excluir_aluno(ra: str):
    excluir_aluno_db(ra)
    return redirect(url_for("alunos_page"))


@app.route("/historico", methods=["GET"])
def historico_page():
    sessoes = obter_sessoes_encerradas_db(limite=20)
    return render_template("historico.html", sessoes=sessoes)


@app.route("/historico/<int:sessao_id>", methods=["GET"])
def historico_sessao(sessao_id: int):
    sessao = obter_sessao_por_id(sessao_id)
    if not sessao:
        return redirect(url_for("historico_page"))
    presencas = obter_presencas_da_sessao(sessao_id)
    return render_template("historico_sessao.html", sessao=sessao, presencas=presencas)


@app.route("/historico/<int:sessao_id>/exportar", methods=["GET"])
def exportar_presenca(sessao_id: int):
    formato = request.args.get("formato", "pdf").lower()
    if formato not in {"pdf", "txt"}:
        formato = "pdf"
    return exportar_presenca_por_sessao(sessao_id, formato=formato)


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
            print(f"[CADASTRO] Aluno cadastrado: RA='{ra}' nome='{nome}'")
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