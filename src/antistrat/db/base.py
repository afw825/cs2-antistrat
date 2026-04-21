# src/antistrat/db/base.py
from sqlalchemy.orm import declarative_base

# All models will inherit from this Base
Base = declarative_base()
