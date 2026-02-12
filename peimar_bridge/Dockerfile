ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
# Nota: Home Assistant sostituirà automaticamente amd64 con l'architettura corretta del Raspberry
FROM $BUILD_FROM

# Installazione dipendenze di sistema e fuso orario
RUN apk add --no-cache python3 py3-pip && \
    pip3 install --upgrade pip && \
    pip3 install requests paho-mqtt --break-system-packages

# Impostazione Fuso Orario Italia
ENV TZ=Europe/Rome
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Disabilita il buffering dei log per vedere tutto in tempo reale
ENV PYTHONUNBUFFERED=1

# Copia lo script nella radice del container
COPY peimar.py /peimar.py

# Avvio dello script
CMD [ "python3", "/peimar.py" ]
