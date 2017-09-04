from kafka import KafkaConsumer
import json
from producer import Message, MessageType
import pathlib
from topic import *

consumer = KafkaConsumer('Sync', group_id='view', bootstrap_servers=['0.0.0.0:9092'])

sync_dir_name = 'Sync2'

def list_topic_and_path():
    topics: set = consumer.topics()
    print(topics)
    print(topic_dict(pickle_file_name))
    for t in topics:
        topic = find_topic(pickle_file_name, t)
        if topic:
            print(topic)


if __name__ == '__main__':
    list_topic_and_path()

    sync_dir = pathlib.Path.home().joinpath(sync_dir_name)
    if not (sync_dir.exists() and sync_dir.is_dir()):
        sync_dir.mkdir()

    for msg in consumer:
        _msg = msg._asdict()
        message = json.loads(_msg['value'])
        m = Message.from_json(message)
        print(m)
        if m.type == MessageType.CREATE:
            sync_file = pathlib.Path.home().joinpath(sync_dir_name).joinpath(m.name)
            with open(sync_file, 'wb') as open_file:
                open_file.write(m.data.encode('latin-1'))
                print('Sync ' + str(sync_file))
            
        if m.type == MessageType.DELETE:
            sync_file = pathlib.Path.home().joinpath(sync_dir_name).joinpath(m.name)
            sync_file.unlink()
            print('Delete ' + str(sync_file))
