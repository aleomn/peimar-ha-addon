import requests
from datetime import date, datetime, timedelta
import paho.mqtt.client as mqtt
import json
import time
import os

class PeimarTester():
    def __init__(self):
        # Percorsi Home Assistant Add-on
        self.OPTIONS_PATH = "/data/options.json"
        self.HISTORY_PATH = "/share/peimar_history.json"
        
        # Carica configurazione dall'interfaccia Add-on
        try:
            with open(self.OPTIONS_PATH, "r") as f:
                conf = json.load(f)
            self.log("✅ Configurazione caricata correttamente")
        except Exception as e:
            self.log(f"❌ Errore caricamento opzioni: {e}")
            conf = {}
        
        # ====== CONFIGURAZIONE PEIMAR (Dalle opzioni Add-on) ======
        self.BASE = "http://www.peimar-portal.com"
        self.USERNAME = conf.get("peimar_user", "")
        self.PASSWORD = conf.get("peimar_pass", "")
        self.PLANT_UID = "5F7C9010-3FE6-40A1-8E59-975D45ED6BC2"
        self.DEVICE_SN = "H1S2602J2050E00358"

        # ====== CONFIGURAZIONE MQTT ======
        self.MQTT_HOST = conf.get("mqtt_host", "core-mosquitto") 
        self.MQTT_PORT = 1883
        self.MQTT_USER = conf.get("mqtt_user", "mqtt")
        self.MQTT_PASS = conf.get("mqtt_pass", "mqttpassword")
        self.MQTT_PREFIX = "peimar"

        # ====== STATO INTERNO ======
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{self.BASE}/portal/login"
        })
        self.plant = {}
        self.live = {}
        self.raw = {}
        self.bean = {}
        self.last_login = datetime.now() - timedelta(hours=7)
        
        # Setup MQTT (Callback V2)
        self.mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqttc.username_pw_set(self.MQTT_USER, self.MQTT_PASS)
        self.mqttc.on_message = self.on_message
        
        try:
            self.mqttc.connect_async(self.MQTT_HOST, self.MQTT_PORT)
            self.mqttc.loop_start()
            self.log("📡 Connessione MQTT avviata")
        except Exception as e:
            self.log(f"❌ Errore MQTT: {e}")

    def log(self, msg):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

    def clean_value(self, value):
        if value is None: return 0.0
        try:
            if isinstance(value, str):
                clean_str = value.replace('%', '').replace('kWh', '').replace('W', '').replace(',', '.').strip()
                return float(clean_str)
            return float(value)
        except: return 0.0
        
    def announce_sensor(self, name, topic, unit="", dev_class=None, state_class="measurement"):
        discovery_topic = f"homeassistant/sensor/{self.MQTT_PREFIX}/{topic.replace('/', '_')}/config"
        payload = {
            "name": f"{name}",
            "state_topic": f"{self.MQTT_PREFIX}/{topic}",
            "unique_id": f"peimar_{self.DEVICE_SN}_{topic.replace('/', '_')}",
            "unit_of_measurement": unit,
            "device": {
                "identifiers": [f"peimar_inverter_{self.DEVICE_SN}"],
                "name": "Inverter Peimar",
                "manufacturer": "Peimar"
            }
        }
        if dev_class: payload["device_class"] = dev_class
        if state_class: payload["state_class"] = state_class
        self.mqttc.publish(discovery_topic, json.dumps(payload), retain=True)
        
    def setup_discovery(self):
        self.log("📣 Annuncio sensori a Home Assistant...")
        # ================== FOTOVOLTAICO =============================
        self.announce_sensor("Potenza Impianto", "pv/solar_power", "KW")
        self.announce_sensor("Potenza PV Live", "pv/power_now", "W", "power", "measurement")
        self.announce_sensor("Potenza S1 Live", "pv/pv1_power", "W", "power", "measurement")
        self.announce_sensor("Potenza S2 Live", "pv/pv2_power", "W", "power", "measurement")
        self.announce_sensor("Tensione S1 Live", "pv/pv1_volt", "V", "voltage", "measurement")
        self.announce_sensor("Tensione S2 Live", "pv/pv2_volt", "V", "voltage", "measurement")
        self.announce_sensor("Corrente S1 Live", "pv/pv1_current", "A", "current", "measurement")
        self.announce_sensor("Corrente S2 Live", "pv/pv2_current", "A", "current", "measurement")
        self.announce_sensor("Energia Prodotta Oggi", "pv/energy_today", "kWh", "energy", "total_increasing")
        # --- DETTAGLI BATTERIA ---
        self.announce_sensor("Capacità Batteria", "battery/capacity", "Ah")
        self.announce_sensor("SOC Batteria", "battery/soc", "%", "battery", "measurement")
        self.announce_sensor("Potenza Batteria Live", "battery/power_live", "W", "power", "measurement")
        self.announce_sensor("Tensione Batteria", "battery/volt", "V", "voltage", "measurement")
        self.announce_sensor("Corrente Batteria", "battery/current", "A", "current", "measurement")
        self.announce_sensor("Energia Caricata in Bat. Oggi", "battery/today_charge_energy", "kWh", "energy", "total_increasing")
        self.announce_sensor("Energia Scaricata da Bat. Oggi", "battery/today_discharge_energy", "kWh", "energy", "total_increasing")
        self.announce_sensor("Stato Batteria", "battery/battery_status", "", None, None)
        # ======= DETTAGLI CARICO CASA / RETE ===============================================================
        self.announce_sensor("Consumo Casa Live", "house/load_live", "W", "power", "measurement")
        self.announce_sensor("Consumo Casa oggi", "house/energy_consumption_today", "kWh", "energy", "total_increasing")
        self.announce_sensor("Energia Importata Oggi", "house/import_today", "kWh", "energy", "total_increasing")
        self.announce_sensor("Energia Esportata Oggi", "house/export_today", "kWh", "energy", "total_increasing")
        self.announce_sensor("Potenza Rete Live", "grid/power", "W", "power", "measurement")
        self.announce_sensor("Tensione Rete Live", "grid/grid_voltage", "V", "voltage", "measurement")
        self.announce_sensor("Corrente Rete Live", "grid/grid_current", "A", "current", "measurement")
        self.announce_sensor("Frequenza Rete Live", "grid/grid_frequency", "Hz", "frequency", "measurement")
        self.announce_sensor("Stato Rete", "grid/grid_status", "", None, None)
        # ========== INVERTER ===================================
        self.announce_sensor("Temperatura Inverter", "inverter/deviceTemp", "°C", "temperature", "measurement")
        # ==================== STATUS =========================================================
        self.announce_sensor("Ultimo Aggiornamento", "status/last_update_time", "", None, None)

    def login(self):
        try:
            self.session.post(f"{self.BASE}/portal/login", 
                              data={"username": self.USERNAME, "password": self.PASSWORD}, timeout=30)
            self.last_login = datetime.now()
            self.log("🔐 Login Peimar OK")
        except Exception as e:
            self.log(f"❌ Login Fallito: {e}")

    def on_message(self, client, userdata, msg):
        try:
            mese_scelto = msg.payload.decode().strip()
            if '-' not in mese_scelto or len(mese_scelto) < 6:
                return

            self.log(f"📩 HA richiede dati per: {mese_scelto}")
            history = self.load_local_history()
            y_req, m_req = mese_scelto.split('-')
            m_req = str(int(m_req)) 

            if y_req in history and m_req in history[y_req]:
                d = history[y_req][m_req]
                self.mqttc.publish("peimar/history/selected/pv", d['pv'], retain=True)
                self.mqttc.publish("peimar/history/selected/use", d['use'], retain=True)
                self.mqttc.publish("peimar/history/selected/buy", d['buy'], retain=True)
                self.mqttc.publish("peimar/history/selected/sell", d['sell'], retain=True)
                self.log(f"✅ Dati di {mese_scelto} pubblicati.")
        except Exception as e:
            self.log(f"❌ Errore callback MQTT: {e}")
    
    # =====================================================================================
    # Scarica i dati dal portale e li conserve nei dizionari plant{}, Live{}, raw{}, bean{}
    # =====================================================================================

    def fetch_data(self):
        ts = int(time.time() * 1000)
        oggi = date.today().isoformat()
        ieri = (date.today() - timedelta(days=1)).isoformat()
        domani = (date.today() + timedelta(days=1)).isoformat()
        try:
            # 1. PLANT
            r = self.session.post(f"{self.BASE}/portal/monitor/site/getPlantDetailInfo", 
                                  data={"plantuid": self.PLANT_UID, "clientDate": date.today().isoformat(), "_t": ts})
            self.plant = r.json().get("plantDetail", {})
            #print(self.plant)
            # print("Dati Plant Ok")

            # 2. LIVE
            r = self.session.get(f"{self.BASE}/portal/monitor/site/getStoreOrAcDevicePowerInfo", 
                                 params={"plantuid": self.PLANT_UID, "devicesn": self.DEVICE_SN, "_t": ts})
            self.live = r.json().get("storeDevicePower", {})
            #print(self.live)
            # print("Dati Live Ok")

            # 3. RAW
            r = self.session.get(f"{self.BASE}/portal/cloudMonitor/deviceInfo/findRawdataPageList", 
                                 params={"deviceSn": self.DEVICE_SN, "deviceType": "1", "timeStr": date.today().isoformat(), "_": ts})
            raw_list = r.json().get("list", [0])
            if raw_list:
                self.raw = raw_list[0]
            #print(self.raw)
            # print("Dati Raw Ok")
            
            # 4. VIEW BEAN (Dati energetici storici/giornalieri)
            url_bean = f"{self.BASE}/portal/monitor/site/getPlantDetailChart2"
            params_bean = {
                "plantuid": self.PLANT_UID,
                "chartDateType": "1",
                "energyType": "0",
                "clientDate": oggi,
                "deviceSnArr": self.DEVICE_SN,
                "chartCountType": "2",
                "previousChartDay": ieri,
                "nextChartDay": domani,
                "chartDay": oggi,
                "elecDevicesn": self.DEVICE_SN,
                "_": ts
            }
            r_bean = self.session.get(url_bean, params=params_bean, timeout=30)
            
            # --- IL CONTROLLO DI SICUREZZA ---
            if r_bean.status_code == 200:
                # Solo se il server risponde "OK" (200), provo a leggere il JSON
                try:
                    self.bean = r_bean.json().get("viewBean")
                    # print("Dati Bean Ok")
                    #print(self.bean)
                except Exception as e:
                    self.log(f"⚠️ Errore nel formato JSON ricevuto: {e}")
                    self.bean = {} # Evita che lo script si rompa se il JSON è malformato
            else:
                self.log(f"⚠️ Portale Peimar momentaneamente non raggiungibile (Status: {r_bean.status_code})")
                self.bean = {} # Reset del bean per non usare dati vecchi o nulli
            
            
            self.log("📡 Dati scaricati correttamente")
           
        except Exception as e:
            self.log(f"❌ Errore fetch: {e}")

    def fetch_full_history(self, start_year=2022):
        self.log(f"🚀 Avvio recupero storico totale dal {start_year}...")
        history = {}
        url = f"{self.BASE}/portal/monitor/site/getPlantDetailChart2"
        now = datetime.now()
        
        for year in range(start_year, now.year + 1):
            history[str(year)] = {} 
            end_month = now.month if year == now.year else 12
            for month in range(1, end_month + 1):
                ts = int(time.time() * 1000)
                month_str = f"{year}-{month:02d}"
                params = {
                    "plantuid": self.PLANT_UID,
                    "chartDateType": "2",
                    "energyType": "0",
                    "clientDate": f"{month_str}-01",
                    "deviceSnArr": self.DEVICE_SN,
                    "chartCountType": "2",
                    "chartMonth": month_str,
                    "chartYear": str(year),
                    "elecDevicesn": self.DEVICE_SN,
                    "_": ts
                }
                try:
                    r = self.session.get(url, params=params, timeout=30)
                    resp_data = r.json()
                    
                    # DEEP DEBUG: Vediamo la prima risposta utile
                    if year == start_year and month == 1:
                        self.log(f"🔍 DEBUG Risposta Portale: {resp_data}")

                    data = resp_data.get("viewBean", {})
                    if data and any(v is not None for v in [data.get("pvElec"), data.get("useElec")]):
                        history[str(year)][str(month)] = {
                            "pv": self.clean_value(data.get("pvElec")),
                            "use": self.clean_value(data.get("useElec")),
                            "buy": self.clean_value(data.get("buyElec")),
                            "sell": self.clean_value(data.get("sellElec"))
                        }
                    else:
                        # Logghiamo solo se non troviamo nulla per capire dove si ferma
                        pass
                    
                    time.sleep(0.5) 
                except Exception as e:
                    self.log(f"⚠️ Errore mese {month_str}: {e}")
        
        # Filtriamo gli anni che sono rimasti vuoti
        final_history = {k: v for k, v in history.items() if v}
        return final_history
            
    def load_local_history(self):
        try:
            with open(self.HISTORY_PATH, "r") as f:
                content = f.read().strip()
                if not content: return {}
                return json.loads(content)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_local_history(self, history):
        try:
            with open(self.HISTORY_PATH, "w") as f:
                json.dump(history, f, indent=4)
            self.log("💾 history.json salvato")
        except Exception as e:
            self.log(f"❌ Errore scrittura storico: {e}")
    
    def print_ordered_history(self, history_data):
        if not history_data:
            self.log("📭 Nessun dato storico da mostrare.")
            return

        print("\n" + "="*75)
        print(f"{'ARCHIVIO STORICO ENERGETICO':^75}")
        print("="*75)
        print(f"{'PERIODO':<15} | {'PRODOTTA':>12} | {'CONSUMATA':>12} | {'ACQUISTATA':>10} | {'VENDUTA':>10}")
        print("-"*75)

        tot_gen = {"pv": 0.0, "use": 0.0, "buy": 0.0, "sell": 0.0}
        
        for y_key in sorted(history_data.keys(), key=lambda x: int(x)):
            months_sorted = sorted(history_data[y_key].keys(), key=lambda x: int(x))
            for m_key in months_sorted:
                d = history_data[y_key][m_key]
                periodo = "{}-{:02d}".format(y_key, int(m_key))
                print(f"{periodo:<15} | {d['pv']:12.1f} | {d['use']:12.1f} | {d['buy']:10.1f} | {d['sell']:10.1f}")
                for k in tot_gen: tot_gen[k] += d.get(k, 0.0)
            print("-"*75)

        print(f"{'TOTALE':<15} | {tot_gen['pv']:12.1f} | {tot_gen['use']:12.1f} | {tot_gen['buy']:10.1f} | {tot_gen['sell']:10.1f}")
        print("="*75 + "\n")

    def update_ha_menu(self, history_data):
        mesi_disponibili = []
        if history_data:
            years_sorted = sorted(history_data.keys(), key=lambda x: int(x))
            for y in years_sorted:
                months_sorted = sorted(history_data[y].keys(), key=lambda x: int(x))
                for m in months_sorted:
                    mesi_disponibili.append("{}-{:02d}".format(y, int(m)))
        
        mesi_disponibili.reverse() 
        self.mqttc.publish("peimar/history/options", json.dumps(mesi_disponibili), retain=True)
        self.log(f"✅ Inviata lista di {len(mesi_disponibili)} mesi a Home Assistant")
    
    def run(self):
        self.login()
        self.setup_discovery()
        time.sleep(2)
        self.mqttc.subscribe("homeassistant/input_select/peimar_history_period/set")
    
    # CARICAMENTO STORIA
        storia = self.load_local_history()
    
    # Se la storia è vuota o il file ha solo {}, scarica tutto
        if not storia or len(storia) == 0:
            storia = self.fetch_full_history(start_year=2022)
            if storia:
                self.save_local_history(storia)
            else:
                self.log("⚠️ Attenzione: Il recupero dati non ha prodotto risultati.")

        self.print_ordered_history(storia)
        self.update_ha_menu(storia)
              
        last_processed_ts = 0   # Per ricordare l'ultima lettura utile
    
        while True:
            # Aggiornamento programmato
            ora_attuale = datetime.now().strftime("%H:%M")
            if ora_attuale == "00:05":
                self.log("📅 Aggiornamento programmato dello storico...")
                nuova_storia = self.fetch_full_history(start_year=2022)
                self.save_local_history(nuova_storia)
                self.update_ha_menu(nuova_storia)
                time.sleep(60) 
        
            if datetime.now() - self.last_login > timedelta(hours=6):
                self.login()
        
            self.fetch_data()
        
            current_ts = self.live.get("updateDate", 0)
            # Aggiunto spazio e controllo di sicurezza
            ultimo_aggiornamento = datetime.fromtimestamp(current_ts / 1000.0).strftime('%H:%M:%S') if current_ts else "N/D"
                
            if current_ts > last_processed_ts:
                # --- DATI NUOVI TROVATI ---
                self.process_and_publish() 
                last_processed_ts = current_ts
            
                next_update_time = (current_ts / 1000.0) + 315
                sleep_time = int(next_update_time - time.time())
            
                if sleep_time < 10:
                    sleep_time = 10
                
                self.log(f"✨ Dati sincronizzati (Orario portale: {ultimo_aggiornamento})")
                self.log(f"⏳ Prossimo controllo tra {int(sleep_time)} secondi.")
            
                for _ in range(sleep_time):
                    time.sleep(1)
            else:
                # --- DATI ANCORA VECCHI (Else ora è allineato correttamente) ---
                self.log("⏳ Il portale non ha ancora rilasciato nuovi dati. Riprovo tra 30 secondi...")
                for _ in range(30):
                    time.sleep(1)

    
    def process_and_publish(self):
        # Conversione orario
        raw_ts = self.live.get("updateDate")
        orario = datetime.fromtimestamp(raw_ts / 1000.0).strftime('%H:%M:%S') if raw_ts else "N/D"
        # print(orario, "status/last_update_time" )

        grid_power = self.live.get("gridPower")
        grid_direction = self.live.get("gridDirection")
        if grid_direction == -1 and grid_power > 0:
            grid_power = -grid_power
            stato_rete = "Immissione"
        elif grid_direction == 1 and grid_power > 0:
            stato_rete = "Esportazione"
        else:
            stato_rete ="Riposo"

        battery_power = self.raw.get("batPower")
        battery_direction = self.live.get("batteryDirection")
        if battery_direction == -1:
            battery_power = -battery_power
            stato_batteria = "Batteria in carica" 
        elif battery_direction == 1:
            stato_batteria = "Batteria in scarica"
        else:
            stato_batteria = "Batteria a riposo"          
        
        energy_consumption_today = self.clean_value(self.raw.get("todayLoadEnergyStr"))
        # print("Energia Consumata oggi", energy_consumption_today)
        battery_voltage = self.raw.get("batVolt")
        
        # Mappatura
        mapping = {
            # ======== FOTOVOLTAICO ======================
            "pv/solar_power": self.live.get("solarPower"),
            "pv/power_now": self.raw.get("nowPrower"),
            "pv/pv1_power": self.raw.get("pV1Power"),
            "pv/pv2_power": self.raw.get("pV2Power"),
            "pv/pv1_volt": self.raw.get("pV1Volt"),
            "pv/pv2_volt": self.raw.get("pV2Volt"),
            "pv/pv1_current": self.raw.get("pV1Curr"),
            "pv/pv2_current": self.raw.get("pV2Curr"),
            "pv/energy_today": self.raw.get("todayPVEnergy"),
            
            # ========= BATTERIA =========================
            "battery/capacity": self.raw.get("batCapicity"),
            "battery/soc": self.raw.get("batEnergyPercent"),
            "battery/power_live": battery_power,
            "battery/volt": self.raw.get("batVolt"),
            "battery/current": self.raw.get("batCurr"),
            "battery/today_charge_energy": self.raw.get("todayBatChgEnergy"),
            "battery/today_discharge_energy": self.raw.get("todayBatDisEnergy"),
            "battery/total_charge_energy": self.raw.get("totalBatChgEnergy"),
            "battery/total_discharge_energy": self.raw.get("totalBatDisEnergy"),
            "battery/battery_status": stato_batteria,
            
            # ================ CARICO CASA / RETE ===================
            "house/load_live": self.raw.get("totalLoadPowerWatt"),
            "house/energy_consumption_today": self.clean_value(self.raw.get("todayLoadEnergyStr")),
            "house/import_today": self.raw.get("todayFeedInEnergy"),
            "house/export_today": self.raw.get("todaySellEnergy"), 
            "grid/power": grid_power,
            "grid/grid_voltage": self.raw.get("rGridVolt"),
            "grid/grid_current": self.raw.get("rGridCurr"),
            "grid/grid_frequency": self.raw.get("rGridFreq"),
            "grid/grid_status": stato_rete,
                        
            # ========== INVERTER ===================================
            "inverter/deviceTemp": self.raw.get("deviceTemp"),

            # ========== ENERGIA ===================================
            
            # ============== STATUS ================================
            "status/last_update_time": orario,
        }
        
        for topic, val in mapping.items():
            if val is not None:
                # Se il topic riguarda tensioni (volt), correnti (curr) o frequenze,
                # forziamo il valore a float e lo arrotondiamo a 2 decimali
                if any(keyword in topic for keyword in ["volt", "current", "freq", "energy"]):
                    try:
                        val = round(float(val), 2)
                    except:
                        pass
                # Pulizia se è stringa con %
                if isinstance(val, str) and "%" in val: 
                    val = float(val.replace('%', '').replace(',','.'))
                self.mqttc.publish(f"{self.MQTT_PREFIX}/{topic}", val, retain=True)
        
        self.log(f"✅ MQTT Aggiornato ({orario})")


if __name__ == "__main__":
    bridge = PeimarTester()
    try:
        bridge.run()
    except KeyboardInterrupt:
        print("Interrotto dall'utente")
    except Exception as e:
        print(f"Errore fatale: {e}")
    
    
