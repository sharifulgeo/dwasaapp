from flask import Flask
from threading import Thread
from scraper import scraper
import os,datetime


app = Flask('app')
app.secret_key = os.urandom(42)

@app.route('/')
def index():
    thread = Thread(target=scraper)
    thread.daemon = True
    thread.start()
    return '<h1>Scraper started at : {0} </h1>'.format(datetime.datetime.now())

app.run(host='0.0.0.0', port=8080)
