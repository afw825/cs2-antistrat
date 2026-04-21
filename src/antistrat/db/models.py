# src/antistrat/db/models.py
from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .base import Base


class Map(Base):
    __tablename__ = "maps"

    map_id = Column(Integer, primary_key=True, autoincrement=True)
    map_name = Column(String, unique=True, nullable=False)
    pos_x = Column(Float, nullable=False)
    pos_y = Column(Float, nullable=False)
    scale = Column(Float, nullable=False)
    radar_image_path = Column(String, nullable=True)

    matches = relationship("Match", back_populates="map")


class Team(Base):
    __tablename__ = "teams"

    team_id = Column(Integer, primary_key=True, autoincrement=True)
    team_name = Column(String, unique=True, nullable=False)

    players = relationship("Player", back_populates="team")


class Player(Base):
    __tablename__ = "players"

    player_id = Column(Integer, primary_key=True, autoincrement=True)
    steam_id = Column(String, unique=True, nullable=False)
    player_name = Column(String, nullable=False)
    team_id = Column(Integer, ForeignKey("teams.team_id"), nullable=True)

    team = relationship("Team", back_populates="players")
    tick_data = relationship("TickData", back_populates="player")


class Match(Base):
    __tablename__ = "matches"

    match_id = Column(Integer, primary_key=True, autoincrement=True)
    demo_file_name = Column(String, nullable=False)
    map_id = Column(Integer, ForeignKey("maps.map_id"), nullable=False)
    match_date = Column(DateTime, nullable=True)

    map = relationship("Map", back_populates="matches")
    rounds = relationship("Round", back_populates="match")


class Round(Base):
    __tablename__ = "rounds"

    round_id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey("matches.match_id"), nullable=False)
    round_number = Column(Integer, nullable=False)
    winner_side = Column(String, nullable=True)  # e.g., 'CT' or 'T'

    match = relationship("Match", back_populates="rounds")
    tick_data = relationship("TickData", back_populates="round")


class TickData(Base):
    __tablename__ = "tick_data"

    tick_id = Column(Integer, primary_key=True, autoincrement=True)
    round_id = Column(Integer, ForeignKey("rounds.round_id"), nullable=False)
    player_id = Column(Integer, ForeignKey("players.player_id"), nullable=False)

    tick = Column(Integer, nullable=False)
    pos_x = Column(Float, nullable=False)
    pos_y = Column(Float, nullable=False)
    pos_z = Column(Float, nullable=False)
    pixel_x = Column(Float, nullable=True)
    pixel_y = Column(Float, nullable=True)

    round = relationship("Round", back_populates="tick_data")
    player = relationship("Player", back_populates="tick_data")
