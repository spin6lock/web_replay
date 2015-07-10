#encoding:utf8
from pymongo import MongoClient
from flask import Flask, request, jsonify

DB_COUNT = 32
dbs = {}
DBNAME = "replay"
COLLECTION = "data"

def _hash(hash_str):
    s = 0
    for i in range(1, len(hash_str)+1):
        c = ord(hash_str[i-1])
        s = s + c * i
    return (s % DB_COUNT) + 1

def _get_collection(replay_id):
    id = _hash(replay_id)
    name = DBNAME + str(id)
    db = dbs[name]
    return db[COLLECTION]

def get_replay(replay_id):
    collection = _get_collection(replay_id)
    t = collection.find_one({"replay_id":replay_id})
    return t


def init():
    client = MongoClient("localhost", 27017)
    for i in range(1, DB_COUNT+1):
        name = DBNAME + str(i)
        dbs[name] = client[name]

init()

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route('/get_replay')
def web_get_replay():
    replay_id = request.args.get("replay_id", "")
    replay_id = replay_id.encode("utf8")
    ret = get_replay(replay_id)
    if not ret:
        return jsonify({})
    return jsonify(**ret)

if __name__ == '__main__':
    app.run(debug=True)
