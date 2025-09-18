import socket
import threading
from datetime import datetime

LISTENER_PORT = 6000

# devices: dict keyed by imei -> { lat, lon, speed, course, timestamp, last_seen, addr }
devices = {}
devices_lock = threading.Lock()

def parse_ccr01_data(data_bytes, addr=None):
    try:
        decoded = data_bytes.decode("utf-8", errors="ignore").strip()
        print(f"[DEBUG] Raw data received: {decoded}")
        if decoded.startswith("<CCR"):
            parts = decoded.split('|')
            imei = parts[1] if len(parts) > 1 else "unknown"
            dmy = parts[3] if len(parts) > 3 else ""
            hms = parts[4] if len(parts) > 4 else ""
            lat_raw = parts[6] if len(parts) > 6 else ""
            lat_dir = parts[7] if len(parts) > 7 else ""
            lon_raw = parts[8] if len(parts) > 8 else ""
            lon_dir = parts[9] if len(parts) > 9 else ""
            # speed and course
            try:
                speed = float(parts[10]) if len(parts) > 10 and parts[10] else 0.0
            except ValueError:
                speed = 0.0
            try:
                course = float(parts[11]) if len(parts) > 11 and parts[11] else 0.0
            except ValueError:
                course = 0.0

            lat = convert_to_decimal(lat_raw, lat_dir)
            lon = convert_to_decimal(lon_raw, lon_dir)

            # parse timestamp from packet, fallback to UTC now
            try:
                timestamp_dt = datetime.strptime((dmy or "") + (hms or ""), "%d%m%y%H%M%S")
                timestamp = timestamp_dt.isoformat()
            except Exception:
                timestamp = datetime.utcnow().isoformat()

            last_seen = datetime.utcnow().isoformat()

            print(f"[TRACKER] IMEI: {imei}| Latitude: {lat}| Longitude: {lon}| Speed: {speed}| Course: {course}| Time: {timestamp}")

            with devices_lock:
                devices[imei] = {
                    "imei": imei,
                    "lat": lat,
                    "lon": lon,
                    "speed": round(speed, 2),
                    "course": round(course, 1),
                    "timestamp": timestamp,
                    "last_seen": last_seen,
                    "addr": f"{addr[0]}:{addr[1]}" if addr else None
                }
            return imei
    except Exception as e:
        print(f"[!] Error parsing: {e}")
    return None

def convert_to_decimal(coord, direction):
    if not coord or '.' not in coord:
        return 0.0
    if direction in ['N', 'S']:
        degrees = int(coord[:2])
        minutes = float(coord[2:])
    else:
        degrees = int(coord[:3])
        minutes = float(coord[3:])
    decimal = degrees + minutes / 60.0
    if direction in ['S', 'W']:
        decimal = -decimal
    return round(decimal, 6)

def client_handler(conn, addr):
    print(f"[+] New connection from {addr}")
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            print(f"[RAW BYTES] {data}")
            parse_ccr01_data(data, addr=addr)
            # NOTE: do NOT close connection — allow client to keep sending
    except Exception as e:
        print(f"[!] Error: {e}")
    finally:
        print(f"[-] Connection closed: {addr}")
        conn.close()

def run_listener(host="0.0.0.0", port=LISTENER_PORT):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print(f"[*] Starting CCR01 server on {host}:{port}")
    s.bind((host, port))
    s.listen(10)
    while True:
        conn, addr = s.accept()
        threading.Thread(target=client_handler, args=(conn, addr), daemon=True).start()

# helper for external modules
def get_all_devices():
    with devices_lock:
        return list(devices.values())

def get_device(imei):
    with devices_lock:
        return devices.get(imei)

if __name__ == "__main__":

    run_listener()
