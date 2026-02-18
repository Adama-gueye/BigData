from flask import Flask, jsonify
from datetime import datetime

app = Flask(__name__)

@app.route("/exchange-rates")
def exchange_rates():
    return jsonify({
        "date": datetime.now().strftime("%Y-%m-%d"),
        "rates": {
            "USD": 1.0,
            "EUR": 0.92,
            "XOF": 610.0
        }
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
