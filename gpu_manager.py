"""GPU resource manager: stop/start Docling to free VRAM for VLM."""
import requests
import time
import logging

logger = logging.getLogger(__name__)

GPU_HOST = "82.22.53.231"
SSH_CMD = "sshpass -p \"Kx9#mVp4\\!wL7nQ2z\" ssh -o StrictHostKeyChecking=no root@{host}".format(host=GPU_HOST)


def _ssh(cmd):
    import subprocess
    full = f"{SSH_CMD} \"{cmd}\""
    r = subprocess.run(full, shell=True, capture_output=True, text=True, timeout=30)
    return r.returncode == 0, r.stdout.strip()


def stop_docling():
    """Stop Docling to free GPU VRAM for VLM."""
    ok, out = _ssh("systemctl stop docling-parser")
    if ok:
        logger.info("Docling stopped (VRAM freed)")
        time.sleep(2)
    return ok


def start_docling():
    """Restart Docling after VLM is done."""
    ok, out = _ssh("systemctl start docling-parser")
    if ok:
        logger.info("Docling restarted")
        # Wait for it to be ready
        for _ in range(10):
            time.sleep(3)
            try:
                r = requests.get(f"http://{GPU_HOST}:5001/health", timeout=5)
                if r.status_code == 200:
                    logger.info("Docling healthy")
                    return True
            except Exception:
                pass
    return False


def docling_is_running():
    try:
        r = requests.get(f"http://{GPU_HOST}:5001/health", timeout=5)
        return r.status_code == 200
    except Exception:
        return False
