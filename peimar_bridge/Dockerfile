ARG BUILD_FROM=homeassistant/amd64-base:latest
# Nota: Home Assistant sostituirà automaticamente amd64 con l'architettura corretta del Raspberry
FROM $BUILD_FROM

# Installazione dipendenze di sistema e fuso orario
RUN apk add --no-cache \
    python3 \
    py3-pip \
    tzdata

# Impostazione Fuso Orario Italia
ENV TZ=Europe/Rome
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Installazione librerie Python necessarie
RUN pip3 install --no-cache-dir requests paho-mqtt

# Disabilita il buffering dei log per vedere tutto in tempo reale
ENV PYTHONUNBUFFERED=1

# Copia lo script nella radice del container
COPY peimar.py /peimar.py

# Avvio dello script
CMD [ "python3", "/peimar.py" ]
