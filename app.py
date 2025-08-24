from flask import Flask

# ვქმნით Flask აპს
app = Flask(__name__)

# მთავარი route (homepage)
@app.route("/")
def home():
    return "Hello from Lodgify → Monday Sync!"
