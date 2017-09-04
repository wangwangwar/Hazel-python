import time
import os
import pathlib
from kafka import SimpleProducer, KafkaClient
import json
from enum import Enum
import hashlib
from datetime import datetime
from message import Message, MessageType
from topic import *


kafka = KafkaClient('172.18.0.1:9092')
producer = SimpleProducer(kafka)


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
#
# Ignore dir
def compare_files(files1: list, files2: list) -> tuple:
    files1_copy = files1[:]
    files2_copy = files2[:]

    files1_copy
    for file in files2:
        if file in files1_copy:
            files1_copy.remove(file)

    for file in files1:
        if file in files2_copy:
            files2_copy.remove(file)

    return (files1_copy, files2_copy)


def filter_dir(dir: pathlib.Path, filenames: list) -> list:
    filenames_copy = filenames[:]

    for filename in filenames:
        file = dir.joinpath(filename)
        if file.is_dir():
            filenames_copy.remove(filename)

    return filenames_copy


def listen_dir(dir: pathlib.Path):
    if not (dir.exists() or dir.is_dir()):
        print('dir ' + dir + ' is not valid')
        return 

    topic = dir.name

    # List dir
    # old_files = os.listdir(dir)
    old_files = []
    while True:
        time.sleep(2)
        _files = os.listdir(dir)
        files = filter_dir(dir, _files)
        result = compare_files(old_files, files)
        print(result)
        messages = build_messages(dir, result)
        for m in messages:
            print(m)
            producer.send_messages(
                topic, json.dumps(m.to_json()).encode('utf-8'))
        old_files = files


def main():
    print(topic_dict(pickle_file_name))
    dir_name = 'Sync'
    sync_dir = pathlib.Path.home().joinpath(dir_name)
    add_topic(pickle_file_name, dir_name, sync_dir)
    listen_dir(sync_dir)
    

if __name__ == '__main__':
    main()