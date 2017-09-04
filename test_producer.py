import unittest
from producer import (
    init_pickle_file,
    delete_pickle_file,
    topic_dict,
    add_topic,
    find_topic
)
import pathlib


class TestProducer(unittest.TestCase):

    pickle_file_name = 'unittest.data'

    def setUp(self):
        init_pickle_file(self.pickle_file_name, True)
    
    def tearDown(self):
        delete_pickle_file(self.pickle_file_name)

    def test_list_topics(self):
        self.assertTrue(topic_dict(self.pickle_file_name) != None)

    def test_add_topic(self):
        sync_dir_name = 'unittest'
        self.assertTrue(add_topic(self.pickle_file_name, sync_dir_name, pathlib.Path.home().joinpath(sync_dir_name)))
        topic = topic_dict(self.pickle_file_name)
        self.assertTrue(find_topic(self.pickle_file_name, sync_dir_name) != None)


if __name__ == '__main__':
    unittest.main()