import json
import binascii
import time
from pathlib import Path

import paho.mqtt.client as mqtt


# ========== Carga de configuración ==========

def load_config(path: str = "config.json") -> dict:
    cfg_path = Path(path)
    if not cfg_path.is_file():
        raise FileNotFoundError(f"No se encontró {cfg_path.resolve()}")
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


config = load_config()

MQTT_HOST = config["mqtt"]["host"]
MQTT_PORT = int(config["mqtt"].get("port", 1883))
MQTT_USER = config["mqtt"].get("username") or None
MQTT_PASS = config["mqtt"].get("password") or None
MQTT_CLIENT_ID = config["mqtt"].get("client_id", "protolink-modbus-bridge")

TOPIC_IN = config["mqtt"]["topic_in"]
TOPIC_OUT = config["mqtt"]["topic_out"]   # todavía no lo usamos, pero ya queda


# ========== Callbacks MQTT ==========

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[OK] Conectado a MQTT {MQTT_HOST}:{MQTT_PORT}")
        print(f"[SUB] Suscrito a: {TOPIC_IN}")
        client.subscribe(TOPIC_IN, qos=0)
    else:
        print(f"[ERR] Error de conexión MQTT, rc={rc}")


def on_message(client, userdata, msg):
    payload = msg.payload  # tipo bytes
    hex_payload = binascii.hexlify(payload).decode("ascii")

    print("=" * 60)
    print(f"[MSG] Tópico: {msg.topic}")
    print(f"[MSG] Bytes recibidos: {len(payload)}")
    print(f"[MSG] HEX: {hex_payload}")

    # En este punto, todavía NO interpretamos Modbus.
    # Solo estamos validando qué formato exacto está enviando el Protolink.


def on_disconnect(client, userdata, rc):
    print(f"[INFO] Desconectado de MQTT, rc={rc}")


# ========== Setup y loop principal ==========

def main():
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)

    if MQTT_USER is not None:
        client.username_pw_set(MQTT_USER, MQTT_PASS)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    print(f"[INIT] Conectando a {MQTT_HOST}:{MQTT_PORT} ...")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    # Loop no bloqueante (permite que en un futuro hagamos más cosas en el main)
    client.loop_start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[STOP] Saliendo...")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
