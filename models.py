"""
Models SQLAlchemy para o Sistema de Controle de Presença NFC com IoT
Define as tabelas: Aluno e Presenca
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

    ra = Column(String(50),primary_key=True, unique=True, nullable=False, index=True)
    nome = Column(String(300), nullable=False)
    id_nfc = Column(String(300), unique=True, nullable=False, index=True)
    data_criacao = Column(DateTime, default=datetime.now, nullable=False)

    # Relacionamento com Presenca
    presencas = relationship("Presenca", back_populates="aluno", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Aluno(ra='{self.ra}', nome='{self.nome}', id_nfc='{self.id_nfc}')>"

    def to_dict(self):
        """Converte o objeto para dicionário (compatível com código existente)."""
        return {
            "ra": self.ra,
            "nome": self.nome,
            "id_nfc": self.id_nfc,
        }


class Presenca(Base):
    """
    Modelo para armazenar registros de presença.
    Cada registro representa uma entrada ou saída de um aluno.
    """
    __tablename__ = "presencas"

    id_presenca = Column(Integer, primary_key=True, autoincrement=True)
    ra = Column(String(50), ForeignKey("alunos.ra"), nullable=False, index=True)
    data_hora = Column(String(19), nullable=False)  # Formato: DD/MM/YYYY HH:MM:SS
    tipo: Column[str] = Column(String(20), nullable=False)  # entrada, saida, erro, ignorado
    criado_em = Column(DateTime, default=datetime.now, nullable=False)

    # Relacionamento com Aluno
    aluno = relationship("Aluno", back_populates="presencas")

    def __repr__(self):
        return f"<Presenca(id={self.id_presenca}, ra={self.ra}, tipo='{self.tipo}', data_hora='{self.data_hora}')>"

    def to_dict(self):
        """Converte o objeto para dicionário (compatível com código existente)."""
        aluno = self.aluno
        return {
            "ra": aluno.ra if aluno else "-",
            "nome": aluno.nome if aluno else "Cartão não identificado",
            "uid": aluno.id_nfc if aluno else "-",
            "data_hora": self.data_hora,
            "tipo": self.tipo,
        }
