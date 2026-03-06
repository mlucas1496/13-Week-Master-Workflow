"""
13WCF Unified Workflow
======================
Combines all 5 weekly processing steps into a single Flask application:
  Step 1: Weekly Balances Rollforward (in-browser)
  Step 2: Activity Aggregator Update
  Step 3: Activity Aggregator Mapper
  Step 4: Activity Rollforward
  Step 5: 13WCF Data Loader (in-browser)

All engine code is bundled in the engines/ directory — no external
folders or files are needed.
"""

import os
import sys
import uuid
import json
import time
import shutil
import threading
import traceback
import importlib.util
from datetime import datetime

from flask import Flask, render_template, request, jsonify, Response, send_file
from werkzeug.utils import secure_filename

# ---------------------------------------------------------------------------
# Path setup: all engines are bundled locally
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

STEP2_ENGINE_DIR = os.path.join(BASE_DIR, "engines", "step2_aggregator")
STEP3_ENGINE_DIR = os.path.join(BASE_DIR, "engines", "step3_mapper")
STEP4_ENGINE_DIR = os.path.join(BASE_DIR, "engines", "step4_rollforward")
STEP5_ENGINE_DIR = os.path.join(BASE_DIR, "engines", "step5_data_loader")

# Step 2 uses `from pipeline.xxx import ...` and `from config import ...`
# so we add its directory to sys.path
if STEP2_ENGINE_DIR not in sys.path:
    sys.path.insert(0, STEP2_ENGINE_DIR)

# Step 4 uses `import stacked_activity_updater` and `import fva_data_updater`
# as sibling imports, so we add its directory too
if STEP4_ENGINE_DIR not in sys.path:
    sys.path.insert(0, STEP4_ENGINE_DIR)

# ---------------------------------------------------------------------------
# Import Step 2 pipeline (Activity Aggregator Update)
# ---------------------------------------------------------------------------
from pipeline.orchestrator import run_pipeline as step2_run_pipeline

# ---------------------------------------------------------------------------
# Import Step 3 mapper (Activity Aggregator Mapper)
# Uses importlib to avoid Flask app conflicts
# ---------------------------------------------------------------------------
_mapper_spec = importlib.util.spec_from_file_location(
    "mapper_engine",
    os.path.join(STEP3_ENGINE_DIR, "mapper.py"),
)
mapper_engine = importlib.util.module_from_spec(_mapper_spec)
_orig_argv = sys.argv
sys.argv = [""]
_mapper_spec.loader.exec_module(mapper_engine)
sys.argv = _orig_argv

# ---------------------------------------------------------------------------
# Import Step 4 modules (Activity Rollforward)
# ---------------------------------------------------------------------------
import stacked_activity_updater
import fva_data_updater

_rollforward_spec = importlib.util.spec_from_file_location(
    "rollforward_engine",
    os.path.join(STEP4_ENGINE_DIR, "rollforward.py"),
)
rollforward_engine = importlib.util.module_from_spec(_rollforward_spec)
sys.argv = [""]
_rollforward_spec.loader.exec_module(rollforward_engine)
sys.argv = _orig_argv

# ---------------------------------------------------------------------------
# Flask app
# ---------------------------------------------------------------------------
app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024 * 1024  # 1 GB
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure the imported rollforward engine's app to use our folders
rollforward_engine.app.config["OUTPUT_FOLDER"] = OUTPUT_DIR
rollforward_engine.app.config["UPLOAD_FOLDER"] = UPLOAD_DIR

# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------
workflow = {
    "files": {},          # key -> {"path": ..., "name": ...}
    "step_status": {      # step_id -> "pending" | "running" | "done" | "error"
        "1": "pending",
        "2": "pending",
        "3": "pending",
        "4": "pending",
        "5": "pending",
    },
    "step_outputs": {},   # step_id -> {"path": ..., "name": ...}
    "step_logs": {},      # step_id -> [log_messages]
    "step_stages": {},    # step_id -> {stage_id: status}
    "step_errors": {},    # step_id -> error_message
    "step_stats": {},     # step_id -> stats dict
}

ALLOWED_EXT = {"xlsx", "xls", "csv"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT


def save_upload(file_obj, key):
    """Save an uploaded file and register it in the workflow."""
    fname = secure_filename(file_obj.filename)
    unique = uuid.uuid4().hex[:8]
    dest = os.path.join(UPLOAD_DIR, f"{unique}_{fname}")
    file_obj.save(dest)
    workflow["files"][key] = {"path": dest, "name": file_obj.filename}
    return dest


def get_file_path(key):
    """Get the path for a registered file (uploaded or output from prior step)."""
    entry = workflow["files"].get(key)
    return entry["path"] if entry else None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    """Return full workflow state."""
    return jsonify({
        "files": {k: v["name"] for k, v in workflow["files"].items()},
        "step_status": workflow["step_status"],
        "step_outputs": {
            k: v["name"] for k, v in workflow["step_outputs"].items()
        },
        "step_stats": workflow["step_stats"],
    })


@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Upload one or more files. Form keys become file registry keys."""
    saved = {}
    for key in request.files:
        f = request.files[key]
        if f and f.filename:
            path = save_upload(f, key)
            saved[key] = f.filename
    return jsonify({"saved": saved})


@app.route("/api/save-output/<step>", methods=["POST"])
def api_save_output(step):
    """Save a browser-processed output file (Steps 1 & 5)."""
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file provided"}), 400
    fname = secure_filename(f.filename)
    unique = uuid.uuid4().hex[:8]
    dest = os.path.join(OUTPUT_DIR, f"step{step}_{unique}_{fname}")
    f.save(dest)
    workflow["step_outputs"][step] = {"path": dest, "name": f.filename}
    workflow["step_status"][step] = "done"

    # Register output as available for subsequent steps
    if step == "1":
        workflow["files"]["step1_weekly_balances_output"] = {
            "path": dest, "name": f.filename
        }
    elif step == "5":
        workflow["files"]["step5_13wcf_output"] = {
            "path": dest, "name": f.filename
        }

    return jsonify({"saved": fname})


# ---------------------------------------------------------------------------
# Step 2: Activity Aggregator Update
# ---------------------------------------------------------------------------

@app.route("/api/run/2", methods=["POST"])
def run_step2():
    if workflow["step_status"]["2"] == "running":
        return jsonify({"error": "Already running"}), 409

    # Check required files
    required = ["s2_prev_week", "s2_bank_statements", "s2_all_transactions",
                 "s2_loan_report", "s2_search_strings", "s2_static_mapping"]
    missing = [k for k in required if not get_file_path(k)]
    if missing:
        return jsonify({"error": f"Missing files: {', '.join(missing)}"}), 400

    workflow["step_status"]["2"] = "running"
    workflow["step_logs"]["2"] = []
    workflow["step_stages"]["2"] = {}
    workflow["step_errors"]["2"] = None

    file_paths = {
        "prev_week": get_file_path("s2_prev_week"),
        "bank_statements": get_file_path("s2_bank_statements"),
        "all_transactions": get_file_path("s2_all_transactions"),
        "loan_report": get_file_path("s2_loan_report"),
        "search_strings": get_file_path("s2_search_strings"),
        "static_mapping": get_file_path("s2_static_mapping"),
    }

    def worker():
        try:
            output_dir = os.path.join(OUTPUT_DIR, "step2")
            os.makedirs(output_dir, exist_ok=True)

            def log(msg):
                workflow["step_logs"]["2"].append(msg)

            def set_stage(stage_id, status):
                workflow["step_stages"]["2"][stage_id] = status

            result = step2_run_pipeline(file_paths, output_dir, log, set_stage)

            workflow["step_outputs"]["2"] = {
                "path": result["file_path"],
                "name": result["file_name"],
            }
            workflow["step_stats"]["2"] = result.get("stats", {})
            workflow["step_status"]["2"] = "done"

            # Register output for Step 3
            workflow["files"]["step2_activity_aggregator_output"] = {
                "path": result["file_path"],
                "name": result["file_name"],
            }

        except Exception as e:
            workflow["step_errors"]["2"] = str(e)
            workflow["step_status"]["2"] = "error"
            workflow["step_logs"]["2"].append(f"ERROR: {e}")
            traceback.print_exc()

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/progress/2")
def progress_step2():
    """SSE endpoint for Step 2 progress."""
    def generate():
        last_log_idx = 0
        last_stages = {}
        while True:
            logs = workflow["step_logs"].get("2", [])
            if len(logs) > last_log_idx:
                for msg in logs[last_log_idx:]:
                    yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
                last_log_idx = len(logs)

            stages = dict(workflow["step_stages"].get("2", {}))
            if stages != last_stages:
                yield f"data: {json.dumps({'type': 'stages', 'stages': stages})}\n\n"
                last_stages = stages

            status = workflow["step_status"]["2"]
            if status == "done":
                stats = workflow["step_stats"].get("2", {})
                yield f"data: {json.dumps({'type': 'done', 'stats': stats})}\n\n"
                break
            elif status == "error":
                yield f"data: {json.dumps({'type': 'error', 'message': workflow['step_errors'].get('2', 'Unknown')})}\n\n"
                break
            time.sleep(0.3)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---------------------------------------------------------------------------
# Step 3: Activity Aggregator Mapper
# ---------------------------------------------------------------------------

@app.route("/api/run/3", methods=["POST"])
def run_step3():
    if workflow["step_status"]["3"] == "running":
        return jsonify({"error": "Already running"}), 409

    # Activity Aggregator: from Step 2 output or manual upload
    agg_path = get_file_path("step2_activity_aggregator_output") or get_file_path("s3_aggregator")
    roll_path = get_file_path("s3_rollforward")

    if not agg_path:
        return jsonify({"error": "Missing Activity Aggregator file (run Step 2 first or upload manually)"}), 400
    if not roll_path:
        return jsonify({"error": "Missing Activity Rollforward file"}), 400

    workflow["step_status"]["3"] = "running"
    workflow["step_logs"]["3"] = []
    workflow["step_errors"]["3"] = None

    def worker():
        try:
            mapper_engine.run_mapping(agg_path, roll_path)

            # Wait for completion
            while mapper_engine.job_state["status"] == "running":
                current_logs = mapper_engine.job_state.get("logs", [])
                for entry in current_logs[len(workflow["step_logs"]["3"]):]:
                    text = entry["text"] if isinstance(entry, dict) else str(entry)
                    workflow["step_logs"]["3"].append(text)
                time.sleep(0.5)

            # Final log sync
            current_logs = mapper_engine.job_state.get("logs", [])
            for entry in current_logs[len(workflow["step_logs"]["3"]):]:
                text = entry["text"] if isinstance(entry, dict) else str(entry)
                workflow["step_logs"]["3"].append(text)

            if mapper_engine.job_state["status"] == "done":
                src_name = mapper_engine.job_state.get("filename", "Activity Aggregator - MAPPED.xlsx")
                src_path = str(mapper_engine.OUTPUT_DIR / src_name)

                dest_name = f"Activity_Aggregator_MAPPED_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                dest_path = os.path.join(OUTPUT_DIR, dest_name)
                shutil.copy2(src_path, dest_path)

                workflow["step_outputs"]["3"] = {
                    "path": dest_path,
                    "name": dest_name,
                }
                workflow["step_stats"]["3"] = mapper_engine.job_state.get("stats", {})
                workflow["step_status"]["3"] = "done"

                # Register for Step 4
                workflow["files"]["step3_mapped_aggregator_output"] = {
                    "path": dest_path,
                    "name": dest_name,
                }
            else:
                workflow["step_errors"]["3"] = mapper_engine.job_state.get("error", "Unknown error")
                workflow["step_status"]["3"] = "error"

        except Exception as e:
            workflow["step_errors"]["3"] = str(e)
            workflow["step_status"]["3"] = "error"
            workflow["step_logs"]["3"].append(f"ERROR: {e}")
            traceback.print_exc()

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/progress/3")
def progress_step3():
    """SSE endpoint for Step 3 progress."""
    def generate():
        last_log_idx = 0
        while True:
            logs = workflow["step_logs"].get("3", [])
            if len(logs) > last_log_idx:
                for msg in logs[last_log_idx:]:
                    yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
                last_log_idx = len(logs)

            pct = mapper_engine.job_state.get("progress", 0)
            yield f"data: {json.dumps({'type': 'progress', 'percent': pct})}\n\n"

            status = workflow["step_status"]["3"]
            if status == "done":
                stats = workflow["step_stats"].get("3", {})
                yield f"data: {json.dumps({'type': 'done', 'stats': stats})}\n\n"
                break
            elif status == "error":
                yield f"data: {json.dumps({'type': 'error', 'message': workflow['step_errors'].get('3', 'Unknown')})}\n\n"
                break
            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---------------------------------------------------------------------------
# Step 4: Activity Rollforward
# ---------------------------------------------------------------------------

@app.route("/api/run/4", methods=["POST"])
def run_step4():
    if workflow["step_status"]["4"] == "running":
        return jsonify({"error": "Already running"}), 409

    weekly_path = get_file_path("step1_weekly_balances_output") or get_file_path("s4_weekly_balances")
    rollforward_path = get_file_path("s3_rollforward") or get_file_path("s4_rollforward")
    aggregator_path = get_file_path("step3_mapped_aggregator_output") or get_file_path("s4_aggregator")
    bth_path = get_file_path("s4_bth")
    fva_1w = get_file_path("s4_fva_1week")
    fva_4w = get_file_path("s4_fva_4week")
    fva_13w = get_file_path("s4_fva_13week")

    if not weekly_path:
        return jsonify({"error": "Missing Weekly Balances file (run Step 1 first or upload manually)"}), 400
    if not rollforward_path:
        return jsonify({"error": "Missing Activity Rollforward file"}), 400

    workflow["step_status"]["4"] = "running"
    workflow["step_logs"]["4"] = []
    workflow["step_errors"]["4"] = None

    def worker():
        try:
            fva_files = {}
            if fva_1w:
                fva_files["1week"] = fva_1w
            if fva_4w:
                fva_files["4week"] = fva_4w
            if fva_13w:
                fva_files["13week"] = fva_13w

            result = rollforward_engine.process_files(
                weekly_file_path=weekly_path,
                rollforward_file_path=rollforward_path,
                bth_file_path=bth_path,
                aggregator_file_path=aggregator_path,
                fva_files=fva_files if fva_files else None,
            )

            workflow["step_logs"]["4"] = result.get("log", [])

            if result.get("success"):
                output_name = result["output_file"]
                output_path = os.path.join(OUTPUT_DIR, output_name)

                workflow["step_outputs"]["4"] = {
                    "path": output_path,
                    "name": output_name,
                }
                workflow["step_stats"]["4"] = result.get("stats", {})
                workflow["step_status"]["4"] = "done"

                workflow["files"]["step4_rollforward_output"] = {
                    "path": output_path,
                    "name": output_name,
                }
            else:
                workflow["step_errors"]["4"] = result.get("error", "Processing failed")
                workflow["step_status"]["4"] = "error"

        except Exception as e:
            workflow["step_errors"]["4"] = str(e)
            workflow["step_status"]["4"] = "error"
            workflow["step_logs"]["4"].append(f"ERROR: {e}")
            traceback.print_exc()

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/progress/4")
def progress_step4():
    """SSE endpoint for Step 4 progress."""
    def generate():
        last_log_idx = 0
        while True:
            logs = workflow["step_logs"].get("4", [])
            if len(logs) > last_log_idx:
                for msg in logs[last_log_idx:]:
                    yield f"data: {json.dumps({'type': 'log', 'message': msg})}\n\n"
                last_log_idx = len(logs)

            status = workflow["step_status"]["4"]
            if status == "done":
                stats = workflow["step_stats"].get("4", {})
                yield f"data: {json.dumps({'type': 'done', 'stats': stats})}\n\n"
                break
            elif status == "error":
                yield f"data: {json.dumps({'type': 'error', 'message': workflow['step_errors'].get('4', 'Unknown')})}\n\n"
                break
            time.sleep(0.5)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ---------------------------------------------------------------------------
# Download any step output
# ---------------------------------------------------------------------------

@app.route("/api/download/<step>")
def download_output(step):
    output = workflow["step_outputs"].get(step)
    if not output:
        return jsonify({"error": "No output available"}), 404
    return send_file(
        output["path"],
        as_attachment=True,
        download_name=output["name"],
    )


# ---------------------------------------------------------------------------
# Step 1: Bank of Canada FX rate proxy (replaces Vite dev proxy)
# ---------------------------------------------------------------------------

@app.route("/api/boc-fx")
def boc_fx_proxy():
    """Proxy Bank of Canada Valet API requests to avoid CORS issues."""
    import requests as req
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")
    url = (
        f"https://www.bankofcanada.ca/valet/observations/group/FX_RATES_DAILY/json"
        f"?start_date={start_date}&end_date={end_date}"
    )
    try:
        resp = req.get(url, timeout=15)
        return Response(resp.content, status=resp.status_code,
                        content_type=resp.headers.get("Content-Type", "application/json"))
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ---------------------------------------------------------------------------
# Serve the Data Loader HTML for Step 5 (bundled locally)
# ---------------------------------------------------------------------------

@app.route("/api/data-loader-html")
def data_loader_html():
    """Serve the 13WCF Data Loader HTML."""
    loader_path = os.path.join(STEP5_ENGINE_DIR, "data_loader.html")
    if os.path.exists(loader_path):
        with open(loader_path, "r") as f:
            return f.read()
    return "<p>Data Loader HTML not found.</p>", 404


# ---------------------------------------------------------------------------
# Reset workflow
# ---------------------------------------------------------------------------

@app.route("/api/reset", methods=["POST"])
def reset_workflow():
    """Reset all workflow state."""
    workflow["files"].clear()
    workflow["step_outputs"].clear()
    workflow["step_logs"].clear()
    workflow["step_stages"].clear()
    workflow["step_errors"].clear()
    workflow["step_stats"].clear()
    for k in workflow["step_status"]:
        workflow["step_status"][k] = "pending"
    # Clean upload/output dirs
    for d in [UPLOAD_DIR, OUTPUT_DIR]:
        for f in os.listdir(d):
            fp = os.path.join(d, f)
            if os.path.isfile(fp):
                os.remove(fp)
            elif os.path.isdir(fp):
                shutil.rmtree(fp)
    return jsonify({"status": "reset"})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  13WCF Unified Workflow")
    print("  http://localhost:8888")
    print("  Press Ctrl+C to quit")
    print("=" * 60)
    app.run(host="127.0.0.1", port=8888, debug=False, threaded=True)
