from flask import Flask
from flaskext.csrf import csrf

app = Flask(__name__)
csrf(app)

import jia.views
