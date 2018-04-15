from flask import Flask, request
app = Flask(__name__, instance_relative_config=True)

@app.route('/', methods=['GET'])
def index():
    return 'hello world'

app.run(port=8080) 