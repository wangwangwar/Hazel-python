from datetime import datetime
from enum import Enum


class MessageType(Enum):
    CREATE = 1
    DELETE = 2


class Message(object):
    type: MessageType
    name: str
    data: bytes
    hash: str
    datetime: datetime

    def __init__(self, type: MessageType, name: str, datetime: datetime, data: bytes = None, hash: str = None):
        self.type = type
        self.name = name
        self.datetime = datetime
        self.data = data
        self.hash = hash

    def to_json(self):
        type: str
        if self.type == MessageType.CREATE:
            return {
                'type': 'CREATE',
                'name': self.name,
                'data': self.data.decode('latin-1'),
                'hash': self.hash,
                'datetime': self.datetime.strftime('%Y-%m-%d %H:%M:%S')
            }
        elif self.type == MessageType.DELETE:
            return {
                'type': 'DELETE',
                'name': self.name,
                'data': None,
                'hash': None,
                'datetime': self.datetime.strftime('%Y-%m-%d %H:%M:%S')
            }

    @staticmethod
    def from_json(json_dict):
        if json_dict['type'] == 'CREATE':
            return Message(
                type=MessageType.CREATE,
                name=json_dict['name'],
                datetime=json_dict['datetime'],
                data=json_dict['data'],
                hash=json_dict['hash'])
        elif json_dict['type'] == 'DELETE':
            return Message(
                type=MessageType.DELETE,
                name=json_dict['name'],
                datetime=json_dict['datetime'])
        else:
            return None

    def __str__(self):
        if self.type == MessageType.CREATE:
            return {
                'type': 'CREATE',
                'name': self.name,
                'hash': self.hash,
                'datetime': self.datetime.strftime('%Y-%m-%d %H:%M:%S')
            }.__str__()
        elif self.type == MessageType.DELETE:
            return {
                'type': 'DELETE',
                'name': self.name,
                'datetime': self.datetime.strftime('%Y-%m-%d %H:%M:%S')
            }.__str__()