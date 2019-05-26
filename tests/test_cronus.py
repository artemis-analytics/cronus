#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Test Class for Artemis MetaStore
"""

import unittest
import logging
import tempfile
import os
from pathlib import Path
from cronus.core.cronus import Cronus

logging.getLogger().setLevel(logging.INFO)


class CronusTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_store(self):
        with tempfile.TemporaryDirectory() as dirpath:
            print(dirpath)
            store = Cronus('teststore', dirpath)
            self.assertEqual(store.name, 'teststore')
            p = Path(dirpath)
            self.assertEqual(store.path, str(p))
            store._save_store()

            loaded = Cronus('teststore', dirpath)
            self.assertEqual(store.store_id, loaded.store_id)


if __name__ == '__main__':
    unittest.main()
