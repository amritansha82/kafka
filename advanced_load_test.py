import socket
import time
import sys
import resource
import gc
gc.disable()

TARGET_HOST = '127.0.0.1'
TARGET_PORT = 9092
NUM_SOURCE_IPS = 25
BATCH_SIZE = 1000

def setup_resource_limits():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        print(f"[SETUP] File descriptor limit raised to {hard}")
    except Exception:
        pass

def run_load_test():
    setup_resource_limits()
    sockets = []
    source_ip_idx = 0
    errors_by_type = {}
    start_time = time.time()
    
    ips = [f"127.0.0.{i+1}" for i in range(NUM_SOURCE_IPS)]
    ip_failed = {ip: False for ip in ips}
    
    print(f"[START] Load test targeting {TARGET_HOST}:{TARGET_PORT}")
    try:
        while True:
            if all(ip_failed.values()):
                print("[LIMIT] All source IPs exhausted.")
                break
                
            src_ip = ips[source_ip_idx % NUM_SOURCE_IPS]
            if ip_failed[src_ip]:
                source_ip_idx += 1
                continue
                
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.bind((src_ip, 0))
                s.connect((TARGET_HOST, TARGET_PORT))
                sockets.append(s)
                source_ip_idx += 1
                if len(sockets) % BATCH_SIZE == 0:
                    rate = len(sockets) / (time.time() - start_time)
                    print(f"  Connected {len(sockets):>7,} clients | {rate:,.0f} conn/s")
                    
            except OSError as e:
                err_key = str(e)
                errors_by_type[err_key] = errors_by_type.get(err_key, 0) + 1
                ip_failed[src_ip] = True
                source_ip_idx += 1
                try:
                    s.close()
                except:
                    pass
    except Exception as e:
        print(f"[ERROR] {e}")

    total = len(sockets)
    print(f"Total Connections: {total}")
    for err, cnt in errors_by_type.items():
        print(f"  {err}: {cnt}")
    
    print("Closing sockets...")
    
if __name__ == "__main__":
    run_load_test()
