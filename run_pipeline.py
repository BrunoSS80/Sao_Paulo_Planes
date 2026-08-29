import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
processes = []


def stop_processes(*_args):
    print("\nEncerrando processos...", flush=True)
    for process in processes:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()


def main():
    parser = argparse.ArgumentParser(
        description="Executa producer localmente e bronze dentro do container Spark."
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="localhost:9092",
        help="Endereco do broker Kafka para o producer local (padrao: localhost:9092).",
    )
    args = parser.parse_args()

    
    signal.signal(signal.SIGINT, stop_processes)
    signal.signal(signal.SIGTERM, stop_processes)

    
    prod_env = os.environ.copy()
    prod_env["KAFKA_BOOTSTRAP_SERVERS"] = args.kafka_bootstrap_servers

    producer_proc = subprocess.Popen(
        [sys.executable, str(PROJECT_ROOT / "src" / "producer.py")],
        cwd=PROJECT_ROOT,
        env=prod_env,
    )
    processes.append(producer_proc)
    print(f"[producer.py] iniciado localmente (PID {producer_proc.pid})", flush=True)

    
    bronze_cmd = [
        "docker", "exec",
        "-u", "root",
        "-it", "spark-master",
        "python3", "/opt/spark/src/bronze.py"
    ]

    bronze_proc = subprocess.Popen(
        bronze_cmd,
        cwd=PROJECT_ROOT,
    )
    processes.append(bronze_proc)
    print(f"[bronze.py] iniciado no container spark-master (PID {bronze_proc.pid})", flush=True)

    # Loop de monitoramento
    try:
        while True:
            for name, proc in [("producer.py", producer_proc), ("bronze.py", bronze_proc)]:
                ret = proc.poll()
                if ret is not None:
                    if ret != 0:
                        print(f"[{name}] finalizou com erro (código {ret}). Encerrando...", flush=True)
                    stop_processes()
                    return ret
            time.sleep(1)
    finally:
        stop_processes()


if __name__ == "__main__":
    raise SystemExit(main())