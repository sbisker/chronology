from flask import jsonify
from flask import render_template
from jia import app

@app.route('/')
def index():
    return render_template("index.html")

