import time
import os
import pathlib
from kafka import SimpleProducer, KafkaClient
import json
from enum import Enum
import hashlib
from datetime import datetime


kafka = KafkaClient('172.18.0.1:9092')
producer = SimpleProducer(kafka)

# Assign a topic
topic = 'my-sync-topic'

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

            
# Build message
def build_messages(dir: pathlib.Path, compare_result: tuple) -> list:
    deleted_files = compare_result[0]
    created_files = compare_result[1]
    messages = []

    for file in created_files:
        messages.append(Message(
            type=MessageType.CREATE,
            name=file,
            data=bin_data(dir.joinpath(file)),
            hash=file_hash(dir.joinpath(file)),
            datetime=datetime.now()))

    for file in deleted_files:
        messages.append(Message(
            type=MessageType.DELETE,
            name=file,
            datetime=datetime.now()))
        
    return messages

def bin_data(path: pathlib.Path):
    with open(path, 'rb') as open_file:
        return open_file.read()

def file_hash(path: pathlib.Path):
    with open(path, 'rb') as open_file:
        read_file = open_file.read()
        sha1_hash = hashlib.sha1(read_file).hexdigest()
        return sha1_hash


# Compare two file list
# Return a tuple with two list, first contains files that `files1` only has,
# second contains files that `files2` only has
def compare_files(files1 : list, files2 : list) -> tuple:
    files1_copy = files1[:]
    files2_copy = files2[:]

    for file in files2:
        if file in files1_copy:
            files1_copy.remove(file)

    for file in files1:
        if file in files2_copy:
            files2_copy.remove(file)

    return (files1_copy, files2_copy)


def listen_dir(dir_name):
    # List dir
    sync_dir = pathlib.Path.home().joinpath(dir_name)
    old_files = os.listdir(sync_dir)
    while True:
        time.sleep(2)
        sync_dir = pathlib.Path.home().joinpath(dir_name)
        files = os.listdir(sync_dir)
        result = compare_files(old_files, files)
        print(result)
        messages = build_messages(sync_dir, result)
        for m in messages:
            print(m.to_json())
            producer.send_messages(topic, json.dumps(m.to_json()).encode('utf-8'))
        old_files = files

if __name__ == '__main__':
    listen_dir('Sync')