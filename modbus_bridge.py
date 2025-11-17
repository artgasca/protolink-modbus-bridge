import json
import binascii
import time
from pathlib import Path
import struct
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
TOPIC_OUT_TEMPLATE = config["mqtt"]["topic_out_template"]
DEVICE_ID_TOPIC_INDEX = int(config["mqtt"].get("device_id_topic_index", 1))


MODBUS_CFG = config.get("modbus", {})
FRAME_TYPE = MODBUS_CFG.get("frame_type", "rtu")  # por ahora solo rtu
UNITS_CFG = MODBUS_CFG.get("units", {})



# ========== Helpers MQTT ==========

def extract_device_id_from_topic(topic: str) -> str | None:
    """
    Extrae el device_id del tópico MQTT usando el índice configurado.
    Ejemplo: protolink/<device_id>/modbus/raw  con index=1
    """
    parts = topic.split("/")
    if len(parts) > DEVICE_ID_TOPIC_INDEX:
        return parts[DEVICE_ID_TOPIC_INDEX]
    return None


# ========== Helpers Modbus ==========

def decode_modbus_rtu_frame(payload: bytes):
    """
    Decodifica una trama Modbus RTU del tipo:
    [addr][func][byte_count][data...][crc_lo][crc_hi]

    Retorna:
    {
      "unit_id": int,
      "function": int,
      "registers": [lista de uint16],
      "crc_ok": bool (por ahora siempre True, si quieres luego chequeamos CRC)
    }
    """
    if len(payload) < 5:
        raise ValueError("Trama demasiado corta para ser Modbus RTU")

    unit_id = payload[0]
    function = payload[1]
    byte_count = payload[2]

    expected_len = 3 + byte_count + 2  # addr+func+bc + data + crc
    if len(payload) != expected_len:
        # No tronamos, pero avisamos
        print(f"[WARN] Longitud inesperada. Esperaba {expected_len}, llegó {len(payload)}")

    data = payload[3:3 + byte_count]
    # crc_lo = payload[-2]
    # crc_hi = payload[-1]
    # TODO: si quieres después validamos CRC

    # Convertimos data en lista de registros de 16 bits big-endian
    if len(data) % 2 != 0:
        raise ValueError("Byte count no es múltiplo de 2, no cuadra con registros de 16 bits")

    registers = []
    for i in range(0, len(data), 2):
        reg = (data[i] << 8) | data[i + 1]
        registers.append(reg)

    return {
        "unit_id": unit_id,
        "function": function,
        "registers": registers,
        "crc_ok": True
    }


def map_registers(unit_id: int, function: int, registers: list):
    """
    Usa el config.json para convertir la lista de registros en un dict
    con nombres, tipos (uint16 / float32) y escala.
    """
    unit_cfg = UNITS_CFG.get(str(unit_id))
    if not unit_cfg:
        return {}, None

    func_cfg = unit_cfg.get("functions", {}).get(str(function))
    if not func_cfg:
        return {}, unit_cfg.get("name")

    reg_defs = func_cfg.get("registers_by_index", [])
    mapped = {}

    for reg_def in reg_defs:
        idx = reg_def["index"]
        if idx >= len(registers):
            continue  # no hay suficientes registros

        raw_val = registers[idx]
        scale = float(reg_def.get("scale", 1.0))
        name = reg_def["name"]
        datatype = reg_def.get("datatype", "uint16")
        word_order = reg_def.get("word_order", "ABCD")

        if datatype == "uint16":
            value = raw_val * scale

        elif datatype == "float32":
            # Necesitamos dos registros: idx y idx+1
            if (idx + 1) >= len(registers):
                # No alcanzan los registros, la ignoramos
                continue

            reg_hi = registers[idx]
            reg_lo = registers[idx + 1]
            fval = regs_to_float32(reg_hi, reg_lo, word_order=word_order)
            value = fval * scale

        else:
            # Tipo no soportado todavía; lo dejamos en crudo
            value = raw_val * scale

        mapped[name] = value

    return mapped, unit_cfg.get("name")



def regs_to_float32(reg_hi: int, reg_lo: int, word_order: str = "ABCD") -> float:
    """
    Convierte dos registros uint16 a un float32 IEEE754.

    reg_hi: primer registro (como lo ves en la lista)
    reg_lo: segundo registro
    word_order:
      - "ABCD": [reg_hi_hi][reg_hi_lo][reg_lo_hi][reg_lo_lo] (big-endian estándar)
      - "DCBA": reversa total
      - "BADC": swap de bytes dentro de cada palabra
      - "CDAB": swap de palabras
    """
    a = (reg_hi >> 8) & 0xFF
    b = reg_hi & 0xFF
    c = (reg_lo >> 8) & 0xFF
    d = reg_lo & 0xFF

    orders = {
        "ABCD": bytes([a, b, c, d]),
        "DCBA": bytes([d, c, b, a]),
        "BADC": bytes([b, a, d, c]),
        "CDAB": bytes([c, d, a, b]),
    }

    data = orders.get(word_order, orders["ABCD"])
    # Interpretamos como float32 big-endian
    return struct.unpack(">f", data)[0]


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

    device_id = extract_device_id_from_topic(msg.topic)

    print("=" * 60)
    print(f"[MSG] Tópico: {msg.topic}")
    print(f"[MSG] Device ID (topic): {device_id}")
    print(f"[MSG] Bytes recibidos: {len(payload)}")
    print(f"[MSG] HEX: {hex_payload}")

    try:
        if FRAME_TYPE == "rtu":
            frame = decode_modbus_rtu_frame(payload)
        else:
            print("[ERR] FRAME_TYPE no soportado por ahora")
            return
    except Exception as e:
        print(f"[ERR] Error decodificando Modbus: {e}")
        return

    unit_id = frame["unit_id"]
    function = frame["function"]
    regs = frame["registers"]

    print(f"[DECODE] unit_id={unit_id}, function={function}, registers={regs}")

    mapped, device_name = map_registers(unit_id, function, regs)

    # Construimos JSON de salida
    ts_ms = int(time.time() * 1000)
    out_obj = {
        "ts": ts_ms,
        "unit_id": unit_id,
        "function": function,
        "device": device_name or f"unit_{unit_id}",
        "raw_registers": regs
    }

    if device_id is not None:
        out_obj["device_id"] = device_id

    if mapped:
        out_obj["values"] = mapped

    out_json = json.dumps(out_obj)
    print(f"[OUT] JSON: {out_json}")

    # Tópico de salida basado en device_id
    if device_id is not None:
        topic_out = TOPIC_OUT_TEMPLATE.format(device_id=device_id)
    else:
        # fallback por si no se pudo parsear: algo genérico
        topic_out = TOPIC_OUT_TEMPLATE.format(device_id=f"unit_{unit_id}")

    print(f"[PUB] -> {topic_out}")
    client.publish(topic_out, out_json, qos=0, retain=False)

  


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
