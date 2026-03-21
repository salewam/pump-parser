"""GPU resource manager: stop/start Docling to free VRAM for VLM."""
import requests
import subprocess
import time
import logging

logger = logging.getLogger(__name__)

GPU_HOST = "82.22.53.231"
SSH_CMD = 'sshpass -p "Kx9#mVp4\\!wL7nQ2z" ssh -o StrictHostKeyChecking=no root@{host}'.format(host=GPU_HOST)


def _ssh(cmd):
    full = f'{SSH_CMD} "{cmd}"'
    try:
        r = subprocess.run(full, shell=True, capture_output=True, text=True, timeout=30)
        return r.returncode == 0, r.stdout.strip()
    except Exception as e:
        logger.error("SSH failed: %s", e)
        return False, ""


def stop_docling():
    """Stop Docling to free GPU VRAM for VLM."""
    ok, out = _ssh("systemctl stop docling-parser && sleep 3 && nvidia-smi --query-compute-apps=used_memory --format=csv,noheader,nounits")
    if ok:
        logger.info("Docling stopped, VRAM after: %s", out)
        time.sleep(10)  # Extra wait for GPU memory to fully release
    return ok


def unload_ollama():
    """Unload Ollama model from VRAM (keep service running)."""
    _ssh('curl -s http://localhost:11434/api/generate -d \'{"model":"qwen2.5vl:7b","keep_alive":0}\' > /dev/null 2>&1')
    logger.info("Ollama model unload requested")
    time.sleep(5)


def warmup_ollama():
    """Load Ollama model into memory before VLM requests."""
    logger.info("Warming up Ollama model...")
    _ssh('curl -s http://localhost:11434/api/generate -d \'{"model":"qwen2.5vl:7b","prompt":"hello","stream":false}\' > /dev/null 2>&1')
    logger.info("Ollama warmup done")
    time.sleep(2)


def start_docling():
    """Restart Docling after VLM is done. First unload Ollama model."""
    unload_ollama()
    time.sleep(3)

    ok, out = _ssh("systemctl start docling-parser")
    if ok:
        logger.info("Docling restarted, waiting for ready...")
        for _ in range(20):
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
