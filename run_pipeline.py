import argparse
import os
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
SCRIPTS = ("producer.py", "bronze.py", "silver.py")
SILVER_STATE_PATHS = (
    PROJECT_ROOT / "data" / "silver" / "checkpoint",
    PROJECT_ROOT / "data" / "silver" / "output",
)
processes = []


def signal_processes(signal_number):
    for process in processes:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal_number)
            except ProcessLookupError:
                pass


def stop_processes(*_args):
    signal_processes(signal.SIGINT)

    deadline = time.monotonic() + 10
    while any(process.poll() is None for process in processes):
        if time.monotonic() >= deadline:
            signal_processes(signal.SIGKILL)
            break
        time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser(
        description="Executa producer, bronze e silver simultaneamente."
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default="localhost:9092",
        help="Endereco do broker Kafka (padrao: localhost:9092).",
    )
    parser.add_argument(
        "--reset-silver-state",
        action="store_true",
        help="Recria a Silver removendo output e checkpoint antes de iniciar.",
    )
    args = parser.parse_args()

    if args.reset_silver_state:
        for path in SILVER_STATE_PATHS:
            shutil.rmtree(path, ignore_errors=True)
        print("Estado da Silver removido; os dados serao reprocessados.", flush=True)

    environment = os.environ.copy()
    environment["KAFKA_BOOTSTRAP_SERVERS"] = args.kafka_bootstrap_servers
    signal.signal(signal.SIGINT, stop_processes)
    signal.signal(signal.SIGTERM, stop_processes)

    for script in SCRIPTS:
        process = subprocess.Popen(
            [sys.executable, str(PROJECT_ROOT / "src" / script)],
            cwd=PROJECT_ROOT,
            env=environment,
            start_new_session=True,
        )
        processes.append(process)
        print(f"[{script}] iniciado (PID {process.pid})", flush=True)

    try:
        while True:
            for script, process in zip(SCRIPTS, processes):
                return_code = process.poll()
                if return_code is not None:
                    if return_code != 0:
                        print(
                            f"[{script}] terminou com erro ({return_code}); "
                            "encerrando os demais processos.",
                            flush=True,
                        )
                    stop_processes()
                    return return_code
            time.sleep(1)
    finally:
        stop_processes()


if __name__ == "__main__":
    raise SystemExit(main())