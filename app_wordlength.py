"""
app_wordlength.py
Flask server cho bai toan WordLength (phan loai tu theo do dai).

Ho tro 2 engine:
  - "spark"  : goi spark-submit wordlength.py <input_dir> <output_dir>
  - "hadoop" : copy file len HDFS, chay Hadoop Streaming voi mapper.py/reducer.py

Cau hinh qua bien moi truong (co gia tri mac dinh, sua neu can):
  HADOOP_STREAMING_JAR  duong dan toi hadoop-streaming jar
  HDFS_INPUT_DIR        thu muc input tren HDFS (se bi xoa/ghi de moi lan chay)
  HDFS_OUTPUT_DIR       thu muc output tren HDFS (se bi xoa/ghi de moi lan chay)

Chay:
    pip install flask
    python3 app_wordlength.py
Mo trinh duyet: http://localhost:5001
"""

import os
import re
import json
import shutil
import subprocess
import tempfile

from flask import Flask, request, jsonify, send_from_directory

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SPARK_SCRIPT = os.path.join(BASE_DIR, "wordlength.py")
MAPPER_SCRIPT = os.path.join(BASE_DIR, "mapper.py")
REDUCER_SCRIPT = os.path.join(BASE_DIR, "reducer.py")

# ---- Cau hinh Hadoop (sua lai cho dung moi truong cua ban) ----
HADOOP_STREAMING_JAR = os.environ.get(
    "HADOOP_STREAMING_JAR",
    "/home/master/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
)
HDFS_INPUT_DIR = os.environ.get("HDFS_INPUT_DIR", "/user/master/wordlength_input")
HDFS_OUTPUT_DIR = os.environ.get("HDFS_OUTPUT_DIR", "/user/master/wordlength_output")

CATEGORY_LABELS = {
    "Rat nho": "Rất nhỏ",
    "Nho": "Nhỏ",
    "Trung binh": "Trung bình",
    "Lon": "Lớn",
}
CATEGORY_ORDER = ["Rat nho", "Nho", "Trung binh", "Lon"]

app = Flask(__name__, static_folder=BASE_DIR, static_url_path="")


@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "index_wordlength.html")


def parse_result_lines(lines):
    """Parse cac dong dang 'Category\tcount' hoac 'Category count' hoac
    "('Category', count)" (dinh dang tuple tu Spark saveAsTextFile) thanh dict."""
    counts = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^\(?'?\"?([A-Za-z ]+?)'?\"?\s*[,\t]\s*(\d+)\)?$", line)
        if m:
            category = m.group(1).strip()
            count = int(m.group(2))
            counts[category] = counts.get(category, 0) + count
    return counts


def build_response(counts):
    total = sum(counts.get(c, 0) for c in CATEGORY_ORDER)
    breakdown = [
        {
            "category": c,
            "label": CATEGORY_LABELS[c],
            "count": counts.get(c, 0),
            "ratio": (counts.get(c, 0) / total) if total else 0,
        }
        for c in CATEGORY_ORDER
    ]
    return {"total_words": total, "breakdown": breakdown}


def run_spark(tmp_dir):
    output_dir = os.path.join(tmp_dir, "output")
    proc = subprocess.run(
        ["spark-submit", SPARK_SCRIPT, tmp_dir, output_dir],
        cwd=BASE_DIR, capture_output=True, text=True, timeout=300,
    )
    if proc.returncode != 0:
        return None, proc.stderr[-3000:]

    lines = []
    if os.path.isdir(output_dir):
        for fname in sorted(os.listdir(output_dir)):
            if fname.startswith("part-"):
                with open(os.path.join(output_dir, fname), "r", encoding="utf-8") as f:
                    lines.extend(f.readlines())

    counts = parse_result_lines(lines)
    if not counts:
        # fallback: parse tu stdout (phan "===== WORD LENGTH RESULT =====")
        counts = parse_result_lines(proc.stdout.splitlines())
    return counts, None


def run_hadoop(tmp_dir):
    # Don dep va tao lai thu muc input/output tren HDFS
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", HDFS_INPUT_DIR],
                    capture_output=True, text=True)
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", HDFS_OUTPUT_DIR],
                    capture_output=True, text=True)
    mkdir_proc = subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_INPUT_DIR],
                                 capture_output=True, text=True)
    if mkdir_proc.returncode != 0:
        return None, "Khong tao duoc thu muc HDFS input:\n" + mkdir_proc.stderr[-2000:]

    put_proc = subprocess.run(
        ["hdfs", "dfs", "-put", os.path.join(tmp_dir, "*"), HDFS_INPUT_DIR],
        shell=False, capture_output=True, text=True,
    )
    # shell=False khong ho tro wildcard "*", dung shell=True rieng cho lenh nay
    if put_proc.returncode != 0:
        put_proc = subprocess.run(
            f"hdfs dfs -put {tmp_dir}/* {HDFS_INPUT_DIR}",
            shell=True, capture_output=True, text=True,
        )
        if put_proc.returncode != 0:
            return None, "Khong copy duoc file len HDFS:\n" + put_proc.stderr[-2000:]

    streaming_proc = subprocess.run(
        [
            "hadoop", "jar", HADOOP_STREAMING_JAR,
            "-input", HDFS_INPUT_DIR,
            "-output", HDFS_OUTPUT_DIR,
            "-mapper", f"python3 {os.path.basename(MAPPER_SCRIPT)}",
            "-reducer", f"python3 {os.path.basename(REDUCER_SCRIPT)}",
            "-file", MAPPER_SCRIPT,
            "-file", REDUCER_SCRIPT,
        ],
        cwd=BASE_DIR, capture_output=True, text=True, timeout=600,
    )
    if streaming_proc.returncode != 0:
        return None, streaming_proc.stderr[-3000:]

    cat_proc = subprocess.run(
        ["hdfs", "dfs", "-cat", f"{HDFS_OUTPUT_DIR}/part-*"],
        capture_output=True, text=True,
    )
    counts = parse_result_lines(cat_proc.stdout.splitlines())
    return counts, None


@app.route("/api/wordlength", methods=["POST"])
def wordlength():
    engine = request.form.get("engine", "spark")
    files = request.files.getlist("textfiles")
    if not files:
        return jsonify({"error": "Chua co file nao duoc gui len"}), 400

    tmp_dir = tempfile.mkdtemp(prefix="wordlength_")
    try:
        for f in files:
            if f.filename:
                f.save(os.path.join(tmp_dir, os.path.basename(f.filename)))

        if engine == "hadoop":
            counts, error = run_hadoop(tmp_dir)
        else:
            counts, error = run_spark(tmp_dir)

        if error:
            return jsonify({"error": error, "engine": engine})

        return jsonify({"engine": engine, **build_response(counts)})
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False)