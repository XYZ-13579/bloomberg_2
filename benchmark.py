import time
import requests
import threading
import psutil
import os

def monitor_resources(duration=10):
    process = psutil.Process(os.getpid())
    print("--- Resource Usage Benchmark ---")
    for _ in range(duration):
        cpu = process.cpu_percent(interval=1.0)
        mem = process.memory_info().rss / 1024 / 1024
        print(f"CPU: {cpu}% | Mem: {mem:.2f} MB")

def test_sse(query="トヨタ"):
    url = f"http://127.0.0.1:5050/search-stream?q={query}"
    print(f"Connecting to SSE: {url}")
    start = time.time()
    
    with requests.get(url, stream=True) as response:
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                print(f"[{time.time() - start:.2f}s] {decoded_line[:100]}")
                if "event: stock" in decoded_line:
                    break

if __name__ == "__main__":
    t = threading.Thread(target=test_sse)
    t.start()
    
    monitor_resources(5)
    t.join()
