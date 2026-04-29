FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir poetry==1.8.3

COPY .streamlit /app/.streamlit
COPY pyproject.toml poetry.lock README.md /app/
COPY src /app/src
COPY data /app/data

RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

EXPOSE 8501

CMD ["streamlit", "run", "src/antistrat/api/main.py", "--server.address=0.0.0.0", "--server.port=8501"]
