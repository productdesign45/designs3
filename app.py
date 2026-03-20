import os
import uuid
import json
import hashlib
import threading
import time
from datetime import datetime

import pytz
import pandas as pd
import requests as http_requests
from flask import Flask, render_template, request, jsonify, Response, stream_with_context

app = Flask(__name__)

# ---------------------------------------------------------------------------
# API SMS Configuration
# ---------------------------------------------------------------------------
API_ACCOUNT = "cs_m9chhg"
API_PASSWORD = "mrpt1Uuk"
SENDER_ID = "teddy"
API_URL = "http://sms.yx19999.com:20003/sendsmsV2"
COUNTRY_CODE = "57"

# Rate‑limiting – 3 concurrent sends (semaphore style)
MAX_CONCURRENT = 3
SEND_INTERVAL = 0.35  # seconds between each send

# ---------------------------------------------------------------------------
# In‑memory stores
# ---------------------------------------------------------------------------
# task progress: { task_id: { total, sent, failed, status, log[] } }
tasks = {}
tasks_lock = threading.Lock()

# session stats (in‑memory – resets on restart)
stats = {"total_sent": 0, "total_failed": 0, "total_campaigns": 0}
stats_lock = threading.Lock()

# uploaded file column cache: { preview_id: { columns, rows_preview, df_json } }
uploads = {}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GSM7_CHARS = set(
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ ÆæßÉ"
    " !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "ÄÖÑÜabcdefghijklmnopqrstuvwxyz"
    "äöñüà§"
)
GSM7_EXT = set("^{}\\[~]|€")


def sms_encoding_info(text):
    """Return encoding type, char count, and segment count."""
    is_gsm = all(c in GSM7_CHARS or c in GSM7_EXT for c in text)
    length = len(text)
    if is_gsm:
        ext_count = sum(1 for c in text if c in GSM7_EXT)
        effective_len = length + ext_count  # extended chars cost 2
        if effective_len <= 160:
            segments = 1
        else:
            segments = -(-effective_len // 153)  # ceil division
        return {"encoding": "GSM-7", "chars": length, "effective_chars": effective_len,
                "max_single": 160, "max_concat": 153, "segments": segments}
    else:
        if length <= 70:
            segments = 1
        else:
            segments = -(-length // 67)
        return {"encoding": "Unicode", "chars": length, "effective_chars": length,
                "max_single": 70, "max_concat": 67, "segments": segments}


def _generate_sign():
    tz = pytz.timezone("Asia/Shanghai")
    now = datetime.now(tz).strftime("%Y%m%d%H%M%S")
    raw = API_ACCOUNT + API_PASSWORD + now
    sign = hashlib.md5(raw.encode()).hexdigest()
    return sign, now


def _send_single_sms(phone, message):
    """Send one SMS. Returns (success: bool, detail: str)."""
    sign, dt = _generate_sign()
    params = {"account": API_ACCOUNT, "sign": sign, "datetime": dt}
    payload = {"senderid": SENDER_ID, "numbers": COUNTRY_CODE + phone, "content": message}
    try:
        r = http_requests.post(API_URL, params=params, json=payload, timeout=15)
        data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        status = data.get("status", r.status_code)
        return True, f"OK ({status})"
    except Exception as exc:
        return False, str(exc)


def _replace_tags(template, row_dict):
    """Replace {tag} placeholders with row values."""
    msg = template
    for key, val in row_dict.items():
        msg = msg.replace("{" + str(key) + "}", str(val).strip())
    return msg


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def get_stats():
    with stats_lock:
        return jsonify(stats)


@app.route("/api/sms-info", methods=["POST"])
def sms_info():
    """Return character / segment info for a message."""
    body = request.get_json(silent=True) or {}
    text = body.get("text", "")
    return jsonify(sms_encoding_info(text))


@app.route("/api/test-sms", methods=["POST"])
def test_sms():
    """Send a single test SMS."""
    body = request.get_json(silent=True) or {}
    phone = (body.get("phone") or "").strip()
    message = (body.get("message") or "").strip()
    if not phone or not message:
        return jsonify({"ok": False, "error": "Teléfono y mensaje son requeridos."}), 400
    success, detail = _send_single_sms(phone, message)
    if success:
        with stats_lock:
            stats["total_sent"] += 1
    else:
        with stats_lock:
            stats["total_failed"] += 1
    return jsonify({"ok": success, "detail": detail})


@app.route("/api/upload-preview", methods=["POST"])
def upload_preview():
    """Upload CSV/TXT/XLSX and return columns + first rows for preview."""
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "No se recibió archivo."}), 400

    fname = f.filename.lower()
    try:
        if fname.endswith(".xlsx") or fname.endswith(".xls"):
            df = pd.read_excel(f, dtype=str)
        elif fname.endswith(".csv"):
            df = pd.read_csv(f, dtype=str)
        elif fname.endswith(".txt") or fname.endswith(".tsv"):
            df = pd.read_csv(f, sep=None, engine="python", dtype=str)
        else:
            return jsonify({"ok": False, "error": "Formato no soportado. Use .xlsx, .csv o .txt"}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Error leyendo archivo: {exc}"}), 400

    df.columns = [c.strip() for c in df.columns]
    df = df.dropna(how="all")

    preview_id = uuid.uuid4().hex[:12]
    uploads[preview_id] = {
        "columns": list(df.columns),
        "total_rows": len(df),
        "preview": df.head(5).fillna("").to_dict(orient="records"),
        "df_json": df.fillna("").to_json(orient="records"),
    }
    return jsonify({
        "ok": True,
        "preview_id": preview_id,
        "columns": list(df.columns),
        "total_rows": len(df),
        "preview": df.head(5).fillna("").to_dict(orient="records"),
    })


@app.route("/api/enviar-bulk", methods=["POST"])
def enviar_bulk():
    """Start a bulk send campaign. Returns task_id immediately."""
    body = request.get_json(silent=True) or {}
    preview_id = body.get("preview_id")
    message_template = body.get("message", "")
    phone_column = body.get("phone_column", "")

    if not preview_id or preview_id not in uploads:
        return jsonify({"ok": False, "error": "Archivo no encontrado. Suba el archivo primero."}), 400
    if not message_template.strip():
        return jsonify({"ok": False, "error": "El mensaje no puede estar vacío."}), 400
    if not phone_column:
        return jsonify({"ok": False, "error": "Seleccione la columna de teléfonos."}), 400

    upload = uploads[preview_id]
    rows = json.loads(upload["df_json"])
    task_id = uuid.uuid4().hex[:12]

    with tasks_lock:
        tasks[task_id] = {
            "total": len(rows),
            "sent": 0,
            "failed": 0,
            "status": "running",
            "log": [],
            "started_at": datetime.utcnow().isoformat(),
        }

    def _run():
        sem = threading.Semaphore(MAX_CONCURRENT)
        results_lock = threading.Lock()

        def _process_row(row):
            phone = str(row.get(phone_column, "")).strip()
            if not phone:
                with results_lock:
                    tasks[task_id]["failed"] += 1
                    tasks[task_id]["log"].append({"phone": phone, "ok": False, "detail": "Sin número"})
                return
            msg = _replace_tags(message_template, row)
            with sem:
                ok, detail = _send_single_sms(phone, msg)
                time.sleep(SEND_INTERVAL)
            with results_lock:
                if ok:
                    tasks[task_id]["sent"] += 1
                else:
                    tasks[task_id]["failed"] += 1
                tasks[task_id]["log"].append({"phone": phone, "ok": ok, "detail": detail})

        threads = []
        for row in rows:
            t = threading.Thread(target=_process_row, args=(row,))
            threads.append(t)
            t.start()
            # Stagger thread creation slightly
            time.sleep(0.05)

        for t in threads:
            t.join()

        with tasks_lock:
            tasks[task_id]["status"] = "done"

        with stats_lock:
            stats["total_sent"] += tasks[task_id]["sent"]
            stats["total_failed"] += tasks[task_id]["failed"]
            stats["total_campaigns"] += 1

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return jsonify({"ok": True, "task_id": task_id})


@app.route("/api/progress/<task_id>")
def progress(task_id):
    """SSE endpoint – streams task progress."""
    def generate():
        while True:
            with tasks_lock:
                task = tasks.get(task_id)
            if not task:
                yield f"data: {json.dumps({'error': 'Task not found'})}\n\n"
                break
            payload = {
                "total": task["total"],
                "sent": task["sent"],
                "failed": task["failed"],
                "status": task["status"],
                "processed": task["sent"] + task["failed"],
            }
            yield f"data: {json.dumps(payload)}\n\n"
            if task["status"] == "done":
                break
            time.sleep(0.5)

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/task-log/<task_id>")
def task_log(task_id):
    """Return the full send log for a task."""
    with tasks_lock:
        task = tasks.get(task_id)
    if not task:
        return jsonify({"ok": False, "error": "Tarea no encontrada."}), 404
    return jsonify({"ok": True, "log": task["log"], "status": task["status"],
                     "sent": task["sent"], "failed": task["failed"], "total": task["total"]})


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
