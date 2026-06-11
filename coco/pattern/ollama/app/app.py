import os

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
MODEL = os.environ.get("MODEL", "llama3.2")


@app.route("/")
def index():
    return render_template("index.html", model=MODEL)


@app.route("/api/chat", methods=["POST"])
def chat():
    prompt = (request.get_json(silent=True) or {}).get("prompt", "").strip()
    if not prompt:
        return jsonify({"error": "Prompt is required"}), 400

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/v1/chat/completions",
            json={
                "model": MODEL,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=120,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        return jsonify({"error": f"Could not reach Ollama: {exc}"}), 502

    data = resp.json()
    reply = data["choices"][0]["message"]["content"]
    return jsonify({"response": reply})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
