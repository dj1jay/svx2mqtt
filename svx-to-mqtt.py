#########################################################################
###########################D#J##1##J#A#Y#################################
################        SvxLink - to - MQTT      ########################
#######                                                         #########
#                                                                       #
#       sende Talker, Callsign, Sprechgruppe, RX / TX Status an MQTT    #
#                                                                       #
#######            FM-Funketz.de by DJ1JAY / Jens               #########
#########################################################################
#########################################################################

#!/usr/bin/env python3
import os
import re
import time
import json
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

# ===========================================================
# KONFIGURATION – HIER ANPASSEN
# ===========================================================
LOGFILE_PATH   = "/var/log/svxlink"   # Pfad zur SvxLink-Logdatei

# MQTT
MQTT_BROKER    = "localhost"
MQTT_PORT      = 1883
MQTT_TOPIC     = "svxlink/talker"
MQTT_CLIENT_ID = "svxlog2mqtt"
MQTT_USERNAME  = None
MQTT_PASSWORD  = None
MQTT_RETAIN    = False
MQTT_QOS       = 0
POLL_INTERVAL  = 0.3

# Welche Quellen parsen?
ENABLE_REFLECTOR = True
ENABLE_TX        = True
ENABLE_RX        = True

# Suchkriterien / Tags im Log
REFLECTOR_TAG = "ReflectorLogic"
TX_TAG        = "Tx1"
RX_TAG        = "Rx1"
# ===========================================================

DT_RE = r"^(?P<date>\d{2}\.\d{2}\.\d{4}) (?P<time>\d{2}:\d{2}:\d{2}): "

# TG-Auswahl
SELECT_RE = re.compile(
    DT_RE + re.escape(REFLECTOR_TAG) + r": Selecting TG #(?P<tg>\d+)\s*$",
    re.IGNORECASE
) if ENABLE_REFLECTOR else None

# Talker start/stop
REFLECTOR_RE = re.compile(
    DT_RE + re.escape(REFLECTOR_TAG) +
    r": Talker (?P<state>start|stop) on TG #(?P<tg>\d+): (?P<call>[A-Za-z0-9\/\-]+)\s*$",
    re.IGNORECASE
) if ENABLE_REFLECTOR else None

# Transmitter
TX_RE = re.compile(
    DT_RE + re.escape(TX_TAG) + r": Turning the transmitter (?P<on>ON|OFF)\s*$",
    re.IGNORECASE
) if ENABLE_TX else None

# Receiver
RX_RE = re.compile(
    DT_RE + re.escape(RX_TAG) + r": The squelch is (?P<open>OPEN|CLOSED)\s*$",
    re.IGNORECASE
) if ENABLE_RX else None


def _qos(q):
    try:
        q = int(q)
    except Exception:
        return 0
    return q if q in (0, 1, 2) else 0


def make_client():
    c = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311
    )
    if MQTT_USERNAME:
        c.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD or None)

    def on_connect(cli, userdata, flags, rc, props=None):
        print(f"[MQTT] Verbunden rc={rc}")

    def on_disconnect(cli, userdata, rc, props=None):
        print(f"[MQTT] Getrennt rc={rc}")

    c.on_connect = on_connect
    c.on_disconnect = on_disconnect
    c.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    c.loop_start()
    return c


def tail_follow(path):
    inode = None
    f = None
    pos = 0

    def open_file():
        nonlocal f, inode, pos
        if f:
            f.close()
        f = open(path, "r", encoding="utf-8", errors="replace")
        inode = os.fstat(f.fileno()).st_ino
        f.seek(0, os.SEEK_END)
        pos = f.tell()

    while True:
        try:
            if f is None:
                open_file()
            line = f.readline()
            if line:
                pos = f.tell()
                yield line.rstrip("\n")
                continue
            time.sleep(POLL_INTERVAL)
            st = os.stat(path)
            if st.st_ino != inode or st.st_size < pos:
                open_file()
        except FileNotFoundError:
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            break


def publish_status(client, topic, status):
    data = json.dumps(status, ensure_ascii=False)
    qos = _qos(MQTT_QOS)
    res = client.publish(topic, data, qos=qos, retain=MQTT_RETAIN)
    res.wait_for_publish()
    print(f"PUB {topic} QoS={qos} Retain={MQTT_RETAIN} {data}")


def main():
    client = make_client()

    state = {
        "time": "",
        "talker": "0",
        "TG": "N0TG",
        "Call": "N0Call",
        "tx": "0",
        "rx": "0",
    }
    selected_tg = None

    print(f"Starte: Folge {LOGFILE_PATH} -> MQTT {MQTT_BROKER}:{MQTT_PORT} Topic {MQTT_TOPIC} QoS={_qos(MQTT_QOS)}")

    try:
        for line in tail_follow(LOGFILE_PATH):
            m = None

            # TG-Auswahl
            if ENABLE_REFLECTOR and SELECT_RE:
                m = SELECT_RE.match(line)
                if m:
                    state["time"] = m.group("time")
                    tg = m.group("tg")
                    if tg == "0":
                        # Nur Zustände zurücksetzen
                        selected_tg = None
                        state.update({"talker": "0", "tx": "0", "rx": "0"})
                        publish_status(client, MQTT_TOPIC, state.copy())
                    else:
                        selected_tg = tg
                        state["TG"] = tg
                    continue

            # Talker
            if ENABLE_REFLECTOR and REFLECTOR_RE:
                m = REFLECTOR_RE.match(line)
                if m:
                    if selected_tg is None or m.group("tg") != selected_tg:
                        continue
                    state["time"] = m.group("time")
                    state["talker"] = "1" if m.group("state") == "start" else "0"
                    state["TG"] = selected_tg
                    state["Call"] = m.group("call")
                    publish_status(client, MQTT_TOPIC, state.copy())
                    continue

            # TX
            if ENABLE_TX and TX_RE:
                m = TX_RE.match(line)
                if m:
                    if selected_tg is None:
                        continue
                    state["time"] = m.group("time")
                    state["tx"] = "1" if m.group("on") == "ON" else "0"
                    publish_status(client, MQTT_TOPIC, state.copy())
                    continue

            # RX
            if ENABLE_RX and RX_RE:
                m = RX_RE.match(line)
                if m:
                    if selected_tg is None:
                        continue
                    state["time"] = m.group("time")
                    state["rx"] = "1" if m.group("open") == "OPEN" else "0"
                    publish_status(client, MQTT_TOPIC, state.copy())
                    continue

    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
