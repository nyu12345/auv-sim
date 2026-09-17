FROM python:3.11-slim

# version name is supplied at runtime
ARG AUV_VERSION=dev
ENV AUV_VERSION=${AUV_VERSION}
ENV WORLD_HOST=host.docker.internal
ENV WORLD_PORT=9000
ENV LOG_DIR=/data

# AUV behavior is specified at runtime
ARG AUV_ENTRY=auv.py
ENV AUV_ENTRY=${AUV_ENTRY}

WORKDIR /app
COPY sim/auv*.py sim/messages.py sim/log.py sim/streams.py sim/fleet.json ./
RUN pip install --no-cache-dir nats-py pydantic mcap

CMD ["sh", "-c", "exec python3 -u $AUV_ENTRY"]
