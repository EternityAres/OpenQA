from flask import Flask, request

import time
import hashlib
import six


app = Flask(__name__)

@app.route('/', methods=['GET'])
def api():
    signature = request.args.get('signature')
    echostr = request.args.get('echostr')
    timestamp = request.args.get('timestamp')
    nonce = request.args.get('nonce')
    token = 'renqin'
    _list = [token, timestamp, nonce]
    _list.sort()
    sha1 = hashlib.sha1()
    
    if six.PY2:
        map(sha1.update ,_list)
    else:
        sha1.update("".join(_list).encode('utf-8'))
    
    hashcode=sha1.hexdigest()
    
    if hashcode == signature:
        return echostr
    return ''


if __name__ == '__main__':
    app.run(host='127.0.0.1',port=80, debug=False)