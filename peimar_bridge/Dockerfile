ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:latest
FROM $BUILD_FROM

# 1. Installazione dipendenze e tzdata (necessario per il fuso orario)
RUN apk add --no-cache \
    python3 \
    py3-pip \
    tzdata && \
    pip3 install --upgrade pip && \
    pip3 install requests paho-mqtt --break-system-packages

# 2. Impostazione Fuso Orario Italia
ENV TZ=Europe/Rome
RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 3. Disabilita il buffering (ottimo per vedere i log subito)
ENV PYTHONUNBUFFERED=1

# 4. ATTENZIONE: Il file su GitHub deve chiamarsi peimar.py
# Se il tuo script si chiama run.py, cambia qui sotto in COPY run.py /peimar.py
COPY peimar.py /peimar.py

# 5. Avvio
CMD [ "python3", "/peimar.py" ]
