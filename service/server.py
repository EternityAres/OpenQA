from flask import Flask, request
from service.receive import parse_xml
from service.reply import TextMsg, ImageMsg, VoiceMsg
import time

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return 'Serving...'
    else:
        str_xml = request.data
        msg = parse_xml(str_xml)
        if msg.MsgType == 'text':
            return TextMsg(msg.FromUserName, msg.ToUserName, msg.Content).send()
        elif msg_type == 'image':
            return TextMsg(msg.FromUserName, msg.ToUserName, '暂时不支持图片消息').send()
        elif msg_type == 'voice':
            return TextMsg(msg.FromUserName, msg.ToUserName, msg.Recognition).send()
        else:
            return TextMsg(msg.FromUserName, msg.ToUserName, '消息类型为'+msg.MsgType).send()
        

if __name__ == '__main__':
    app.run(host='127.0.0.1',port=80, debug=False)