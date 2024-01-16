import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
# Import the CogStack class from your module
from cogstack_search_methods.cogstack_v8_lite import CogStack
from credentials import *
from elasticsearch.helpers import ScanError


class TestCogStack(unittest.TestCase):
    @classmethod
    def __init__(self):
        print("Test suite init")
        print(hosts, username)
        self.cogstack = CogStack(hosts=hosts, username = username, password=password)
        print(self.cogstack)
    
    
        # Calculate the date one week ago from the current date
        one_week_ago = datetime.now() - timedelta(days=7)
        formatted_date = one_week_ago.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

        # Construct the query with a range filter
        self.test_query=query = {
            'query': {
                'range': {
                    'updatetime': {
                        'gte': formatted_date
                    }
                }
            }
        }
    
    def setUpClass(cls):
        
        # Set up any resources needed for the test cases
        pass

    @classmethod
    def tearDownClass(cls):
        # Clean up resources after all test cases have run
        pass

    def setUp(self):
        # Create an instance of CogStack for each test case
        self.cogstack = CogStack(hosts=hosts, username = username, password=password)

    def tearDown(self):
        # Clean up after each test case
        pass

    def test_init_with_basic_auth(self):
        # Test CogStack initialization with basic authentication
        with patch('builtins.input', side_effect=[username, password]):
            cogstack = CogStack(hosts=hosts, username = username, password=password)
            self.assertIsNotNone(cogstack.elastic)

    def test_init_with_api_auth(self):
        # Test CogStack initialization with API authentication
        with patch('builtins.input', side_effect=[api_username, api_password]):
            cogstack = CogStack(hosts=hosts, api=True)
            self.assertIsNotNone(cogstack.elastic)

    def test_get_docs_generator(self):
        
        
        # Test get_docs_generator method
        index = 'epr_documents'
        #query = {'query': {'match_all': {}}}
        es_gen = self.cogstack.get_docs_generator(index=[index], query=self.test_query, es_gen_size=800, request_timeout=300)
        self.assertIsNotNone(es_gen)

        # Test with invalid index, should raise ScanError
        with self.assertRaises(ScanError):
            invalid_index = 'invalid_index'
            es_gen_invalid = self.cogstack.get_docs_generator(index=[invalid_index], query=self.test_query,)
            list(es_gen_invalid)

    def test_cogstack2df(self):
        # Test cogstack2df method
        

        
        index = 'epr_documents'
        #query = {'query': {'match_all': {}}}
        df = self.cogstack.cogstack2df(index=index, query=self.test_query)
        self.assertIsInstance(df, pd.DataFrame)

        # Test with invalid index, should raise ScanError
        with self.assertRaises(ScanError):
            invalid_index = 'invalid_index'
            df_invalid = self.cogstack.cogstack2df(index=invalid_index, query=self.test_query)
            self.assertIsNone(df_invalid)

if __name__ == '__main__':
    unittest.main()
