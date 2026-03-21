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
    """Stop Docling to free GPU VRAM for VLM. Then warmup VLM."""
    ok, out = _ssh("systemctl stop docling-parser")
    if ok:
        logger.info("Docling stopped")
        time.sleep(10)

    # Warmup: send real page image to VLM to trigger Ollama model load.
    # First request takes 30-60s as model loads into VRAM.
    logger.info("Warming up VLM (loading model ~40s)...")
    for attempt in range(3):
        try:
            import base64, fitz, glob
            # Find any PDF to render a warmup page from
            pdfs = glob.glob("/root/ONIS/catalogs/*.pdf") + glob.glob("/root/pump_parser/uploads/*.pdf")
            warmup_img = ""
            if pdfs:
                doc = fitz.open(pdfs[0])
                pix = doc[0].get_pixmap(matrix=fitz.Matrix(1.0, 1.0))  # low res
                warmup_img = base64.b64encode(pix.tobytes("png")).decode()
                doc.close()
            if not warmup_img:
                warmup_img = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="

            r = requests.post(
                f"http://{GPU_HOST}:8000/analyze",
                data={"image": warmup_img, "task": "extract_pumps"},
                timeout=300,  # Model load can take 200s+ first time
            )
            if r.status_code == 200:
                body = r.json() if r.text else {}
                has_error = bool(body.get("error"))
                if not has_error:
                    logger.info("VLM warmup OK (attempt %d, %.0fs)", attempt + 1, r.elapsed.total_seconds())
                    return True
                else:
                    # HTTP 200 but Ollama returned error inside — model still loading
                    logger.warning("VLM warmup attempt %d: model not ready (%s)", attempt + 1, str(body.get("error", ""))[:80])
            else:
                logger.warning("VLM warmup attempt %d: HTTP %s", attempt + 1, r.status_code)
        except requests.exceptions.Timeout:
            logger.warning("VLM warmup attempt %d: timeout", attempt + 1)
        except Exception as e:
            logger.warning("VLM warmup attempt %d: %s", attempt + 1, e)
        time.sleep(10)

    logger.error("VLM warmup failed after 3 attempts")
    return False


def start_docling():
    """Restart Docling after VLM is done. Kill Ollama model first."""
    # Restart Ollama to fully release VRAM
    _ssh("systemctl restart ollama")
    logger.info("Ollama restarted to release VRAM")
    time.sleep(5)

    ok, out = _ssh("systemctl start docling-parser")
    if ok:
        logger.info("Docling starting, waiting for ready...")
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
