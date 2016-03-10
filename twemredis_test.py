#!/usr/bin/env python
"""
unit tests for twemredis.py
"""
import unittest
import twemredis
import mockredis


class TestTwemRedis(twemredis.TwemRedis):
    test_canonical_keys = [
        '1', '7', '4', '8', '26', '10', '12', '18', '21', '13',
    ]
    """
    Special TwemRedis sub-class for testing. The _load_config method
    is overriden.
    """
    def _load_config(self, config_file):
        self._num_shards = 10
        self._master_name_base = 'tdb'
        self._shards = {}
        self._sentinels = []
        self._canonical_keys = self.compute_canonical_keys()
        for i in range(0, self.num_shards()):
            self._shards[i] = mockredis.mock_strict_redis_client()
            # for testing
            self._shards[i].set('shard_num', i)


class TwemRedisTests(unittest.TestCase):
    def setUp(self):
        self.tr = TestTwemRedis('test')

    def test_config_override(self):
        # Basic check by calling num_shards()
        self.assertEquals(10, self.tr.num_shards())

    def test_get_key(self):
        self.assertEquals("friend_request:{123456}",
                          self.tr.get_key("friend_request", "123456"))
        # twemredis should not care if it's a number or a word
        self.assertEquals("friend_request:{banana}",
                          self.tr.get_key("friend_request", "banana"))

    def test_get_canonical_key(self):
        # obtained this value empirically
        self.assertEquals(7, self.tr.get_shard_num_by_key_id("123456"))
        # '18' is the key at index 7 in our test canonical key array
        self.assertEquals('18', self.tr.get_canonical_key_id("123456"))
        # get_canonical_key should use get the canonical key id above
        self.assertEquals("canceled:{18}",
                          self.tr.get_canonical_key("canceled", "123456"))

    def test_get_canonical_key_string(self):
        # obtained this value empirically
        self.assertEquals(6, self.tr.get_shard_num_by_key_id("banana"))
        # '12' is the key at index 6 in our test canonical key array
        self.assertEquals('12', self.tr.get_canonical_key_id("banana"))
        # get_canonical_key should use get the canonical key id above
        self.assertEquals("canceled:{12}",
                          self.tr.get_canonical_key("canceled", "banana"))

    def test_get_invalid_shard_raises_error(self):
        try:
            # This should throw an exception.
            self.tr.get_shard_by_num(-1)
        except ValueError:
            self.assertTrue(True)
        else:
            print("ValueError was not raised.")
            self.assertTrue(False)

    def test_auto_sharding_get_set(self):
        self.tr.set('12345', 'bananas')
        # verify we get back what we wrote
        self.assertEquals('bananas', self.tr.get('12345'))

    def test_auto_sharding_shard_numbers(self):
        self.tr.set('12345', 'bananas')
        shard = self.tr.get_shard_by_key('12345')
        # verify we get back what we wrote
        self.assertEquals('bananas', shard.get('12345'))
        # get the shard number and verify it's what the shard says
        shard_num = self.tr.get_shard_num_by_key('12345')
        self.assertEquals(str(shard_num), shard.get('shard_num'))

    def test_compute_canonical_keys(self):
        canonical_keys = self.tr.compute_canonical_keys()
        for i in range(0, len(canonical_keys)):
            key_id = canonical_keys[i]
            shard_num = self.tr.get_shard_num_by_key_id(key_id)
            # check that this direct access key id's index is the
            # same as the shard's index
            self.assertEquals(i, shard_num)
            ckey_id = self.tr.get_canonical_key_id_by_shard_num(shard_num)
            # check that this direct access key id is the same as
            # the canonical key id
            self.assertEquals(str(key_id), ckey_id)
            # check that this mock shard's stored 'shard_num' is
            # the shard we think we're on
            fetched_num = self.tr.get_shard_by_num(shard_num).get('shard_num')
            self.assertEquals(str(shard_num), fetched_num)

    def test_compute_canonical_keys_fails(self):
        # simulate failure to compute enough direct access keys
        try:
            self.tr.compute_canonical_keys(0)
        except:
            self.assertTrue(True)
        else:
            self.assertTrue(False)
