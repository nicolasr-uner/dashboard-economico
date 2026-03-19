from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, ForeignKey, Enum
from sqlalchemy.orm import relationship
from datetime import datetime
import enum
from models.db import Base


class DataTypeEnum(enum.Enum):
    REAL_OFFICIAL = 'REAL_OFFICIAL'
    PROJECTION = 'PROJECTION'
    ESTIMATION = 'ESTIMATION'


class Country(Base):
    __tablename__ = 'dim_country'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    code = Column(String(5), unique=True, nullable=False)
    flag_emoji = Column(String(10))

    variables = relationship("MacroVariable", back_populates="country")


class MacroVariable(Base):
    __tablename__ = 'dim_variable'

    id = Column(Integer, primary_key=True, index=True)
    country_id = Column(Integer, ForeignKey('dim_country.id'), nullable=False)
    name = Column(String(200), nullable=False)
    description = Column(String)
    source_url = Column(String)
    css_selector = Column(String(500))
    frequency = Column(String(20), default='monthly')
    is_dynamic = Column(Boolean, default=False)
    unit = Column(String(50))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # ── Nuevos campos v2: conectores y metadata ───────────────────────────
    connector_type = Column(String(20), default='SCRAPER')   # 'API', 'SCRAPER', 'MANUAL'
    api_provider = Column(String(50))    # 'banrep', 'banxico', 'bcb', 'fred', 'xm', 'world_bank'
    api_serie_id = Column(String(200))   # ID de la serie en la API fuente
    last_successful_fetch = Column(DateTime)
    fetch_error_count = Column(Integer, default=0)
    category = Column(String(50), default='macro')  # 'macro', 'energy', 'fiscal', 'external'

    country = relationship("Country", back_populates="variables")
    historical_data = relationship("TimeSeriesData", back_populates="variable", cascade="all, delete-orphan")
    consensus_forecasts = relationship("ConsensusForecast", back_populates="variable")


class TimeSeriesData(Base):
    __tablename__ = 'fact_timeseries'

    # Llave compuesta para TimescaleDB (Tiempo + Identificadores de metadata)
    date = Column(DateTime, primary_key=True, nullable=False)
    variable_id = Column(Integer, ForeignKey('dim_variable.id'), primary_key=True, nullable=False)
    data_type = Column(Enum(DataTypeEnum), primary_key=True, default=DataTypeEnum.REAL_OFFICIAL)

    source_id = Column(Integer, nullable=True)
    value = Column(Float, nullable=False)
    version_timestamp = Column(DateTime, default=datetime.utcnow)
    is_anomaly = Column(Boolean, default=False)

    variable = relationship("MacroVariable", back_populates="historical_data")


class AIAnalysisLog(Base):
    __tablename__ = 'ai_analysis_log'

    id = Column(Integer, primary_key=True, index=True)
    variable_id = Column(Integer, ForeignKey('dim_variable.id'))
    detected_change = Column(Float)
    ai_verdict = Column(String(20))
    justification = Column(String)
    news_context = Column(String)
    analyzed_at = Column(DateTime, default=datetime.utcnow)
    risk_level = Column(String(10))
    recommendation = Column(String)


class ConsensusForecast(Base):
    """Proyecciones macroeconómicas de bancos, analistas e instituciones."""
    __tablename__ = 'dim_consensus_forecast'

    id = Column(Integer, primary_key=True, index=True)
    variable_id = Column(Integer, ForeignKey('dim_variable.id'), nullable=False)
    source_institution = Column(String(200), nullable=False)   # 'Bancolombia', 'BanRep', 'Focus BCB', etc.
    forecast_date = Column(DateTime, nullable=False)            # Fecha en que se publicó la proyección
    target_date = Column(DateTime, nullable=False)              # Fecha a la que se refiere la proyección
    forecast_value = Column(Float, nullable=False)
    scenario = Column(String(20), default='base')              # 'base', 'optimista', 'pesimista'
    notes = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

    variable = relationship("MacroVariable", back_populates="consensus_forecasts")
