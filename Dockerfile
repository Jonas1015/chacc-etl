FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    supervisor \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir gunicorn gevent-websocket

COPY . .

RUN mkdir -p /var/log/supervisor /etc/supervisor/conf.d

RUN echo "[supervisord]\n\
nodaemon=true\n\
user=root\n\
\n\
[program:chacc-etl-ui]\n\
command=gunicorn --bind 0.0.0.0:5000 --workers 4 --worker-class geventwebsocket.gunicorn.workers.GeventWebSocketWorker wsgi:application\n\
directory=/app\n\
user=app\n\
autostart=true\n\
autorestart=true\n\
redirect_stderr=true\n\
stdout_logfile=/var/log/supervisor/chacc-etl-ui.log\n\
stdout_logfile_maxbytes=50MB\n\
stdout_logfile_backups=3\n\
\n\
[program:luigi-daemon]\n\
command=luigid --background --logdir /app/logs\n\
directory=/app\n\
user=app\n\
autostart=true\n\
autorestart=true\n\
redirect_stderr=true\n\
stdout_logfile=/var/log/supervisor/luigi-daemon.log\n\
stdout_logfile_maxbytes=50MB\n\
stdout_logfile_backups=3" > /etc/supervisor/conf.d/app.conf

RUN mkdir -p logs data

RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/ || exit 1

CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/app.conf"]