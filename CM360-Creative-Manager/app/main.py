from flask import Flask, request, jsonify
import os

from creative_fields_script import main as run_script

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health_check():
    return "Service is running", 200

@app.route("/run", methods=["POST"])
def run():
    try:
        # Optional: simple security check
        expected_token = os.getenv("TRIGGER_TOKEN")
        incoming_token = request.headers.get("Authorization")

        if expected_token and incoming_token != f"Bearer {expected_token}":
            return jsonify({"error": "Unauthorized"}), 401

        run_script()

        return jsonify({"status": "success"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)