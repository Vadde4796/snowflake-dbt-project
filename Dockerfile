FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/root/.dbt

WORKDIR /app

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir "dbt-snowflake>=1.3,<2.0"

COPY . /app

ENTRYPOINT ["dbt"]
CMD ["--version"]
