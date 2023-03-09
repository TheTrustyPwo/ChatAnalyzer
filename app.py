import json
import os
import uuid
import zipfile
from threading import Thread

import redis
from flask import Flask, render_template, request, redirect, jsonify
from werkzeug.utils import secure_filename
from flask_pymongo import PyMongo
from flask_redis import FlaskRedis

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
app.config['UPLOADS_FOLDER'] = 'data'
app.config["MONGO_URI"] = os.getenv('MONGO')
mongo = PyMongo(app)

from analyzer import Analyzer


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/analyze', methods=['POST'])
def analyze():
    analysis_id = uuid.uuid4()
    analyzer = Analyzer(str(analysis_id))

    file = request.files["file"]
    filename = os.path.join(analyzer.path, secure_filename(file.filename))
    file.save(filename)
    with zipfile.ZipFile(filename, "r") as zip_ref:
        zip_ref.extractall(analyzer.path)

    analyzer.start()

    return redirect(f'/analysis/{analysis_id}')


@app.route('/status/<uuid:analysis_id>')
def get_analysis_status(analysis_id):
    """
    API endpoint to get the progress of the analysis task.
    """
    status = mongo.db.progress.find_one_or_404({"_id": str(analysis_id)})
    return status


@app.route('/analysis/<uuid:analysis_id>/raw')
def get_raw_analysis(analysis_id):
    analysis = mongo.db.analyses.find_one_or_404({"_id": str(analysis_id)})
    return analysis


@app.route('/analysis/<uuid:analysis_id>')
def view_analysis(analysis_id):
    return render_template('analysis.html', id=analysis_id)


if __name__ == '__main__':
    app.run()
