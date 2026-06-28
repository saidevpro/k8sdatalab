FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY src/itineo/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/itineo /srv/itineo

EXPOSE 8000

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "8", "app.wsgi:app"]

