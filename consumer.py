from kafka import KafkaConsumer
import json
from producer import Message, MessageType
import pathlib

consumer = KafkaConsumer('my-sync-topic', group_id='view', bootstrap_servers=['0.0.0.0:9092'])

sync_dir_name = 'Sync2'

if __name__ == '__main__':

    sync_dir = pathlib.Path.home().joinpath(sync_dir_name)
    if not (sync_dir.exists() and sync_dir.is_dir()):
        sync_dir.mkdir()

    for msg in consumer:
        _msg = msg._asdict()
        message = json.loads(_msg['value'])
        print(message)
        m = Message.from_json(message)
        print(m)
        if m.type == MessageType.CREATE:
            sync_file = pathlib.Path.home().joinpath(sync_dir_name).joinpath(m.name)
            with open(sync_file, 'wb') as open_file:
                open_file.write(m.data.encode('latin-1'))
                print('Synced ' + m.name)
            