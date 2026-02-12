ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base:latest
FROM $BUILD_FROM

# Installiamo solo lo stretto necessario senza aggiornare pip
RUN apk add --no-cache \
    python3 \
    py3-pip \
    tzdata

# Installazione librerie con flag di sicurezza
RUN pip3 install --no-cache-dir requests paho-mqtt --break-system-packages

# Impostazione Fuso Orario
ENV TZ=Europe/Rome
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV PYTHONUNBUFFERED=1

# Assicurati che il file nel repo si chiami peimar.py
COPY peimar.py /peimar.py

CMD [ "python3", "/peimar.py" ]
