import xml.etree.ElementTree as ET


def parse_xml(web_data):
    if len(web_data) == 0:
        return None
    
    xml_data = ET.fromstring(web_data)
    msg_type = xml_data.find('MsgType').text
    
    if msg_type == 'event':
        pass
    elif msg_type == 'text':
        return TextMsg(xml_data)
    elif msg_type == 'image':
        return ImageMsg(xml_data)
    elif msg_type == 'voice':
        return VoiceMsg(xml_data)
    else:
        pass


class Msg(object):
    def __init__(self, xml_data):
        self.ToUserName = xml_data.find('ToUserName').text
        self.FromUserName = xml_data.find('FromUserName').text
        self.CreateTime = xml_data.find('CreateTime').text
        self.MsgType = xml_data.find('MsgType').text
        self.MsgId = xml_data.find('MsgId').text


class TextMsg(Msg):
    def __init__(self, xml_data):
        super().__init__(xml_data)
        self.Content = xml_data.find('Content').text


class ImageMsg(Msg):
    def __init__(self, xml_data):
        super().__init__(xml_data)
        self.PicUrl = xml_data.find('PicUrl').text
        self.MediaId = xml_data.find('MediaId').text


class VoiceMsg(Msg):
    def __init__(self, xml_data):
        super().__init__(xml_data)
        self.MediaId = xml_data.find('MediaId').text
        self.Format = xml_data.find('Format').text
        self.Recognition = xml_data.find('Recognition').text