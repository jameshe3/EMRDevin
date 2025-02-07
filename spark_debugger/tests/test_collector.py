#!/usr/bin/env python3
import unittest
from collectors.spark_info_collector import SparkInfoCollector

class TestSparkInfoCollector(unittest.TestCase):
    def setUp(self):
        self.collector = SparkInfoCollector("test-host", "test-password")
    
    def test_initialization(self):
        self.assertEqual(self.collector.host, "test-host")
        self.assertEqual(self.collector.password, "test-password")

if __name__ == '__main__':
    unittest.main()
