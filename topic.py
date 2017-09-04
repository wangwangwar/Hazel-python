import pathlib
import pickle


pickle_file_name = 'topic.data'

def init_pickle_file(file_name: str, force: bool = False) -> pathlib.Path:
    path = pathlib.Path.cwd().joinpath(file_name)
    if force or (not (path.exists() and path.is_file())):
        with open(path, 'wb') as pickle_data:
            pickle.dump({}, pickle_data)
            pickle_data.close()
    return path


def delete_pickle_file(file_name: str):
    path = pathlib.Path.cwd().joinpath(file_name)
    path.unlink()


# Add topic
def add_topic(pickle_file_name: str, topic_name: str, sync_dir: pathlib.Path) -> list:
    path = init_pickle_file(pickle_file_name)
    topic: dict
    with open(path, 'rb') as pickle_data:
        topic = pickle.load(pickle_data)
        topic[topic_name] = str(sync_dir.resolve())

    with open(path, 'wb') as pickle_data:
        pickle.dump(topic, pickle_data)
        return topic


# List topics
def topic_dict(pickle_file_name: str) -> dict:
    path = init_pickle_file(pickle_file_name)
    with open(path, 'rb') as pickle_data:
        topic = pickle.load(pickle_data)
        return topic


def find_topic(pickle_file_name: str, topic_name: str) -> dict:
    topic = topic_dict(pickle_file_name)
    try:
        return topic[topic_name]
    except:
        return None
    
    