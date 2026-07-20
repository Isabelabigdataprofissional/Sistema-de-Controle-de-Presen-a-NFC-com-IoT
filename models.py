"""
Models SQLAlchemy para o Sistema de Chamada Escolar NFC com IoT.
Define as tabelas: Aluno, SessaoChamada e Presenca.
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Aluno(Base):
    """
    Modelo para armazenar dados dos alunos.
    Cada aluno possui um UID NFC único associado.
    """
    __tablename__ = "alunos"

    ra = Column(String(50), primary_key=True, unique=True, nullable=False, index=True)
    nome = Column(String(300), nullable=False)
    id_nfc = Column(String(300), unique=True, nullable=False, index=True)
    data_criacao = Column(DateTime, default=datetime.now, nullable=False)

    presencas = relationship("Presenca", back_populates="aluno", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Aluno(ra='{self.ra}', nome='{self.nome}', id_nfc='{self.id_nfc}')>"

    def to_dict(self):
        return {
            "ra": self.ra,
            "nome": self.nome,
            "id_nfc": self.id_nfc,
        }


class SessaoChamada(Base):
    """
    Modelo para armazenar sessões de chamada escolar.
    """
    __tablename__ = "sessoes_chamada"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data = Column(String(10), nullable=False)  # Formato: DD/MM/YYYY
    hora_inicio = Column(String(8), nullable=False)  # Formato: HH:MM:SS
    hora_fim = Column(String(8), nullable=True)
    status = Column(String(20), nullable=False)  # ABERTA, ENCERRADA
    disciplina = Column(String(200), nullable=False, default="")
    turma = Column(String(100), nullable=False, default="")
    professor = Column(String(200), nullable=False, default="")

    presencas = relationship("Presenca", back_populates="sessao", cascade="all, delete-orphan")

    def __repr__(self):
        return (
            f"<SessaoChamada(id={self.id}, status='{self.status}', data='{self.data}', "
            f"hora_inicio='{self.hora_inicio}', disciplina='{self.disciplina}', turma='{self.turma}', "
            f"professor='{self.professor}')>"
        )

    def to_dict(self):
        return {
            "id": self.id,
            "data": self.data,
            "hora_inicio": self.hora_inicio,
            "hora_fim": self.hora_fim,
            "status": self.status,
            "disciplina": self.disciplina,
            "turma": self.turma,
            "professor": self.professor,
        }


class Presenca(Base):
    """
    Modelo para armazenar registros de presença.
    Cada registro representa a presença de um aluno em uma sessão de chamada.
    """
    __tablename__ = "presencas"

    id_presenca = Column(Integer, primary_key=True, autoincrement=True)
    ra = Column(String(50), ForeignKey("alunos.ra"), nullable=False, index=True)
    sessao_id = Column(Integer, ForeignKey("sessoes_chamada.id"), nullable=True, index=True)
    data_hora = Column(String(19), nullable=False)  # Formato: DD/MM/YYYY HH:MM:SS
    tipo = Column(String(20), nullable=False)
    criado_em = Column(DateTime, default=datetime.now, nullable=False)

    aluno = relationship("Aluno", back_populates="presencas")
    sessao = relationship("SessaoChamada", back_populates="presencas")

    def __repr__(self):
        return f"<Presenca(id={self.id_presenca}, ra={self.ra}, sessao_id={self.sessao_id}, tipo='{self.tipo}', data_hora='{self.data_hora}')>"

    def to_dict(self):
        aluno = self.aluno
        return {
            "ra": aluno.ra if aluno else "-",
            "nome": aluno.nome if aluno else "Cartão não identificado",
            "uid": aluno.id_nfc if aluno else "-",
            "data_hora": self.data_hora,
            "tipo": self.tipo,
            "sessao_id": self.sessao_id,
        }
