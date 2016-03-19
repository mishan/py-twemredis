#!/usr/bin/env python
"""
unit tests for twemredis.py
"""
import unittest
import twemredis
import mockredis
import yaml

test_yaml = """
sentinels:
  - sentinel01.example.com
  - sentinel02.example.com
  - sentinel03.example.com
num_shards: 10
shard_name_format: tdb{0:03d}
hash_tag: "{}"
"""
shard_name_format = 'tdb{0:03d}'  # per above
# generated for 10 shard config as above
test_canonical_keys = [
    '1', '7', '4', '8', '26', '10', '12', '18', '21', '13',
]


class TestTwemRedis(twemredis.TwemRedis):
    """
    Special TwemRedis sub-class for testing. The _load_config and
    _init_redis_shard methods are overriden.
    """
    def _parse_config(self, config):
        return yaml.load(config)

    # create mockredis shard instances
    def _init_redis_shards(self):
        self._shards = {}
        for shard_num in range(0, self.num_shards()):
            mock_shard = mockredis.mock_strict_redis_client()
            # for testing
            mock_shard.set('shard_num', shard_num)
            mock_shard.set('shard_name',
                           self.get_shard_name(shard_num))
            self._shards[shard_num] = mock_shard


class TwemRedisTests(unittest.TestCase):
    def setUp(self):
        self.tr = TestTwemRedis(test_yaml)

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
        # verify te shard name is what we're expecting
        shard_name = shard_name_format.format(shard_num)
        self.assertEquals(shard_name, shard.get("shard_name"))

    def test_auto_sharding_keyword_args(self):
        self.tr.zadd('testset', 1, 'foo')
        self.tr.zadd('testset', 2, 'bar')
        results = self.tr.zrange('testset', 0, -1, withscores=True)
        self.assertEquals(2, len(results))
        foo = results[0]
        self.assertEquals(2, len(foo))
        bar = results[1]
        self.assertEquals(2, len(bar))
        self.assertEquals('foo', foo[0])
        self.assertEquals(1.0, foo[1])
        self.assertEquals('bar', bar[0])
        self.assertEquals(2.0, bar[1])

    def test_compute_canonical_key_ids(self):
        canonical_keys = self.tr.compute_canonical_key_ids()
        for i in range(0, len(canonical_keys)):
            key_id = canonical_keys[i]
            shard_num = self.tr.get_shard_num_by_key_id(key_id)
            # check that this direct access key id's index is the
            # same as the shard's index
            self.assertEquals(i, shard_num)
            ckey_id = self.tr.get_canonical_key_id_for_shard(shard_num)
            # check that this direct access key id is the same as
            # the canonical key id
            self.assertEquals(str(key_id), ckey_id)
            # check that this mock shard's stored 'shard_num' is
            # the shard we think we're on
            fetched_num = self.tr.get_shard_by_num(shard_num).get('shard_num')
            self.assertEquals(str(shard_num), fetched_num)
            # check against our expected test canonical key array
            self.assertEquals(test_canonical_keys[i], canonical_keys[i])

    def test_compute_canonical_keys_fails(self):
        # simulate failure to compute enough direct access keys
        try:
            self.tr.compute_canonical_keys(0)
        except:
            self.assertTrue(True)
        else:
            print("ValueError was not raised.")
            self.assertTrue(False)

    def test_get_shard_name(self):
        for shard_num in range(0, self.tr.num_shards()):
            expected_shard_name = shard_name_format.format(shard_num)
            shard_name = self.tr.get_shard_name(shard_num)
            # verify the shard name is what we expect
            self.assertEquals(expected_shard_name, shard_name)
            # verify the shard contains the shard num and name we expect.
            shard = self.tr.get_shard_by_num(shard_num)
            self.assertEquals(str(shard_num), shard.get('shard_num'))
            self.assertEquals(expected_shard_name, shard.get('shard_name'))

    def test_keys_all_shards(self):
        for shard_num in range(0, self.tr.num_shards()):
            shard = self.tr.get_shard_by_num(shard_num)
            shard.set('foo'+str(shard_num), 'bar')
        keys = self.tr.keys('foo*')
        for shard_num in range(0, self.tr.num_shards()):
            self.assertTrue('foo'+str(shard_num) in keys[shard_num])
