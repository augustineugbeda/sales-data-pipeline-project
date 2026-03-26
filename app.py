from flask import Flask, render_template, request, flash, jsonify
from google.cloud import storage
from datetime import datetime
import os

app = Flask(__name__)
app.secret_key = "super-secret-key-change-in-production"

# ================== CONFIG ==================
BUCKET_NAME = "ost_sales_data"          # ← CHANGE THIS
UPLOAD_FOLDER_PREFIX = ""
#"uploads/"


def upload_to_gcs(file, original_filename):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    blob_name = f"{UPLOAD_FOLDER_PREFIX}{timestamp}_{original_filename}"

    blob = bucket.blob(blob_name)
    file.seek(0)
    blob.upload_from_file(file, content_type=file.content_type)

    public_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{blob_name}"
    return blob_name, public_url


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    if 'file' not in request.files:
        return jsonify({"error": "No file selected"}), 400

    file = request.files['file']

    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    try:
        blob_name, public_url = upload_to_gcs(file, file.filename)
        
        return jsonify({
            "success": True,
            "message": "File uploaded successfully!",
            "filename": file.filename,
            "stored_as": blob_name,
            "url": public_url
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)