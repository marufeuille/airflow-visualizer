from flask import Flask
import os
app = Flask(__name__, static_folder='./output')

if __name__ == "__main__":
    # Flaskのマッピング情報を表示
    print(app.url_map)
    app.run(host=os.getenv("APP_ADDRESS", 'localhost'), \
    port=os.getenv("APP_PORT", 3000))