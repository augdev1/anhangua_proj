import os
import socket
import subprocess
import sys
import time
import webbrowser
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 8000
FRONTEND_HOST = "127.0.0.1"
FRONTEND_PORT = 5500


def _wait_for_port(host: str, port: int, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.25)
    return False


def _start_process(cmd: list[str], cwd: Path) -> subprocess.Popen:
    create_flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
    return subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=create_flags,
    )


def main() -> int:
    print("Iniciando Anhangá...\n")

    backend_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "api:app",
        "--host",
        BACKEND_HOST,
        "--port",
        str(BACKEND_PORT),
        "--reload",
    ]

    frontend_cmd = [
        sys.executable,
        "-m",
        "http.server",
        str(FRONTEND_PORT),
        "--bind",
        FRONTEND_HOST,
    ]

    backend_proc = _start_process(backend_cmd, PROJECT_ROOT)
    frontend_proc = _start_process(frontend_cmd, PROJECT_ROOT)

    backend_ok = _wait_for_port(BACKEND_HOST, BACKEND_PORT)
    frontend_ok = _wait_for_port(FRONTEND_HOST, FRONTEND_PORT)

    if not backend_ok or not frontend_ok:
        print("Falha ao iniciar serviços. Verifique dependências e portas em uso.")
        for proc in (backend_proc, frontend_proc):
            if proc.poll() is None:
                proc.terminate()
        return 1

    app_url = f"http://{FRONTEND_HOST}:{FRONTEND_PORT}/index.html"

    print(f"Backend ativo em: http://{BACKEND_HOST}:{BACKEND_PORT}")
    print(f"Frontend: {app_url}")
    print("Login de teste padrão: admin / 123456")

    webbrowser.open(app_url)

    print("\nServiços em execução. Pressione Ctrl+C para encerrar.")

    try:
        while True:
            if backend_proc.poll() is not None:
                print("Backend encerrou inesperadamente.")
                break
            if frontend_proc.poll() is not None:
                print("Servidor frontend encerrou inesperadamente.")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for proc in (backend_proc, frontend_proc):
            if proc.poll() is None:
                proc.terminate()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
