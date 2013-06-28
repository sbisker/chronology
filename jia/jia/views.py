from flask import jsonify
from flask import render_template
from jia import app

@app.route('/')
def index():
    # TODO(meelap)
    # Get the client's user id
    # Get their saved charts and put them in the views list
    return render_template("index.html")

