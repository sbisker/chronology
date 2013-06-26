from flask import jsonify
from flask import render_template
from jia import app

@app.route('/')
def index():
    # Get the client's user id
    # Get their saved charts and put them in the views list
    # Put the form to create a new table/graph at the end of views
    return render_template("index.html")

