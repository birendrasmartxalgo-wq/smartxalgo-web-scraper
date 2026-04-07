import os
import json
import threading
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO
from scraper import fetch_news, save_news, background_scraper, enrich_items

# =========================
# CONFIG
# =========================
OUTPUT_FILE = "market_news.json"

# =========================
# FLASK INIT
# =========================
app = Flask(__name__, static_folder="frontend", static_url_path="")
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# =========================
# LOAD SAVED NEWS
# =========================
def load_saved_news():
    try:
        with open(OUTPUT_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []

# =========================
# API ROUTE - SCRAPE ON DEMAND
# =========================
@app.route("/scrape", methods=["GET"])
def scrape():
    """When user clicks Start Scraping, fetch fresh news and return"""
    news = fetch_news()
    if news:
        news = enrich_items(news)
        save_news(news)
    all_news = load_saved_news()
    return jsonify(all_news)

# =========================
# API ROUTE - GET SAVED NEWS
# =========================
@app.route("/news", methods=["GET"])
def get_news():
    data = load_saved_news()
    return jsonify(data)

# =========================
# SERVE FRONTEND
# =========================
@app.route("/")
def serve_home():
    return send_from_directory(app.static_folder, "index.html")

@app.route("/<path:path>")
def serve_static(path):
    return send_from_directory(app.static_folder, path)

# =========================
# BACKGROUND SCRAPER WRAPPER
# =========================
def start_scraper():
    background_scraper(socketio)

# =========================
# RUN SERVER
# =========================
if __name__ == "__main__":
    print("Starting Flask + WebSocket Server...")
    scraper_thread = threading.Thread(target=start_scraper, daemon=True)
    scraper_thread.start()
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=False, allow_unsafe_werkzeug=True)
