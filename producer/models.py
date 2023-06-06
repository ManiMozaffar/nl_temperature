from sqlalchemy import Column, Integer, String, Float, Table, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


association_table = Table(
    'association',
    Base.metadata,
    Column('clinic_id', Integer, ForeignKey('clinics.id')),
    Column('client_id', Integer, ForeignKey('clients.id')),
    Column('status', Boolean)
)


class Clinic(Base):
    __tablename__ = "clinics"

    id = Column(Integer, primary_key=True, index=True)
    api_urls = Column(String, nullable=True)
    clients = relationship(
        "Client", secondary=association_table,
        back_populates="clinics"
    )


class Client(Base):
    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, index=True)
    temperature_degree = Column(Float, nullable=True)
    clinics = relationship(
        "Clinic", secondary=association_table,
        back_populates="clients"
    )
