#!/usr/bin/env python
"""
twemproxy/nutcracker sharded Redis library for Python.
- Capable of dealing with twemproxy sharded Redis configuration schemes.
- Talks to the Sentinels to obtain the Redis shards
"""

import hashlib
import re
import redis
from redis.sentinel import Sentinel
import yaml


class TwemRedis:
    """
    These Redis functions cannot be called indirectly on a TwemRedis
    instance. If they are to be used, then the operations must be
    performed on the shards directly.
    """
    # TODO: Support mget
    disallowed_sharded_operations = ['hscan', 'keys', 'scan', 'sscan', 'zscan']

    def __init__(self, config_file):
        self._load_config(config_file)

        self._hash_start = self._hash_tag[0]
        self._hash_stop = self._hash_tag[1]
        self._canonical_keys = self.compute_canonical_keys()

        self._init_redis_shards()

    def _load_config(self, config_file):
        self._config = yaml.load(file(config_file, 'r'))
        self._shard_name_format = self._config['shard_name_format']
        self._sentinels = self._config['sentinels']
        self._num_shards = int(self._config['num_shards'])
        self._hash_tag = self._config['hash_tag']

    def _init_redis_shards(self):
        """
        init_redis_shards is used internally to connect to the Redis sentinels
        and populate self.shards with the redis.StrictRedis instances
        """
        self._shards = {}
        sentinel_client = Sentinel(
            [(h, 8422) for h in self._sentinels], socket_timeout=1.0)
        for shard_num in range(0, self.num_shards()):
            shard_name = self._shard_name_format.format(shard_num)
            self._shards[shard_num] = sentinel_client.master_for(
                shard_name, socket_timeout=1.0)
        self._sentinel_client = sentinel_client

    def num_shards(self):
        """
        num_shards returns the number of Redis shards in this cluster.
        """
        return self._num_shards

    def get_key(self, key_type, key_id):
        """
        get_key constructs a key given a key type and a key id.
        Keyword arguments:
        key_type -- the type of key (e.g.: 'friend_request')
        key_id -- the key id (e.g.: '12345')

        returns a string representing the key
        (e.g.: 'friend_request:{12345}')
        """
        return "{0}:{1}{2}{3}".format(key_type, self._hash_start, key_id,
                                      self._hash_stop)

    def get_shard_by_key(self, key):
        """
        get_shard_by_key returns the Redis shard given a key.
        Keyword arguments:
        key -- the key (e.g. 'friend_request:{12345}')

        If the key contains curly braces as in the example, then portion inside
        the curly braces will be used as the key id. Otherwise, the entire key
        is the key id.
        returns a redis.StrictRedis connection
        """
        key_id = self._get_key_id_from_key(key)

        return self.get_shard_by_key_id(key_id)

    def get_shard_num_by_key(self, key):
        """
        get_shard_num_by_key returns the Redis shard number givne a key.
        Keyword arguments:
        key -- the key (e.g. 'friend_request:{12345}')

        See get_shard_by_key for more details as this method behaves the same
        way.
        """

        key_id = self._get_key_id_from_key(key)

        return self.get_shard_num_by_key_id(key_id)

    def get_shard_by_key_id(self, key_id):
        """
        get_shard_by_key_id returns the Redis shard given a key id.
        Keyword arguments:
        key_id -- the key id (e.g. '12345')

        This is similar to get_shard_by_key(key) except that it will not search
        for a key id within the curly braces.
        returns a redis.StrictRedis connection
        """
        return self.get_shard_by_num(self.get_shard_num_by_key_id(key_id))

    def get_shard_num_by_key_id(self, key_id):
        """
        get_shard_num_by_key_id returns the Redis shard number (zero-indexed)
        given a key id.
        Keyword arguments:
        key_id -- the key id (e.g. '12345' or 'anythingcangohere')

        This method is critical in how the Redis cluster sharding works. We
        emulate twemproxy's md5 distribution algorithm.
        """
        m = hashlib.md5(str(key_id)).hexdigest()
        # Below is borrowed from
        # https://github.com/twitter/twemproxy/blob/master/src/hashkit/nc_md5.c
        val = (int(m[0:2], 16) |
               int(m[2:4], 16) << 8 |
               int(m[4:6], 16) << 16 |
               int(m[6:8], 16) << 24)

        return val % self.num_shards()

    def get_canonical_key(self, key_type, key_id):
        """
        get_canonical_key returns the canonical form of a key given a key id.
        For example, '12345' maps to shard 6. The canonical key at index 6
        (say '12') is the canonical key id given the key id of '12345'. This
        is useful for sets that need to exist on all shards.
        Keyword arguments:
        key_type -- the type of key (e.g. 'canceled')
        key_id -- the key id (e.g. '12345')

        returns the canonical key string (e.g. 'canceled:{12}')
        """
        return self.get_key(key_type, self.get_canonical_key_id(key_id))

    def get_canonical_key_id(self, key_id):
        """
        get_canonical_key_id is used by get_canonical_key, see the comment
        for that method for more explanation.
        Keyword arguments:
        key_id -- the key id (e.g. '12345')

        returns the canonical key (e.g. '12')
        """
        return self._canonical_keys[self.get_shard_num_by_key_id(key_id)]

    def get_canonical_key_id_by_shard_num(self, shard_num):
        return self._canonical_keys[shard_num]

    def get_shard_by_num(self, shard_num):
        """
        get_shard_by_num returns the shard at index shard_num.
        Keyword arguments:
        shard_num -- The shard index

        Returns a redis.StrictRedis connection or throws an exception.
        """
        if shard_num < 0 or shard_num >= self.num_shards():
            raise ValueError("requested invalid shard "+str(shard_num))
        return self._shards[shard_num]

    def _get_key_id_from_key(self, key):
        """
        _get_key_id_from_key returns the key id from a key, if found. otherwise
        it just returns the key to be used as the key id.
        Keyword arguments:
        key -- The key to derive the ID from. If curly braces are found in the
               key, then the contents of the curly braces are used as the
               key id for the key.
        """
        key_id = key

        regex = '{0}([^{1}]*){2}'.format(self._hash_start, self._hash_stop,
                                         self._hash_stop)
        m = re.search(regex, key)
        if m is not None:
            key_id = m.group(1)

        return key_id

    def compute_canonical_keys(self, search_amplifier=100):
        canonical_keys = {}
        num_shards = self.num_shards()
        # Guarantees enough to find all keys without running forever
        num_iterations = (num_shards**2) * search_amplifier
        for key_id in range(1, num_iterations):
            shard_num = self.get_shard_num_by_key(str(key_id))
            if shard_num in canonical_keys:
                continue
            canonical_keys[shard_num] = str(key_id)
            if len(canonical_keys) == num_shards:
                break

        if len(canonical_keys) != num_shards:
            raise ValueError("Failed to compute enough keys. " +
                             "Wanted %d, got %d (search_amp=%d).".format(
                                 num_shards, len_canonical_keys,
                                 search_amplifier))

        return canonical_keys

    def mget(self, args):
        key_map = {}
        results = {}
        for key in args:
            shard_num = self.get_shard_num_by_key(str(key))
            if shard_num not in key_map:
                key_map[shard_num] = []
            key_map[shard_num].append(key)

        # TODO: parallelize
        for shard_num in range(0, self.num_shards()):
            shard = self.get_shard_by_num(shard_num)
            results[shard_num] = shard.mget(key_map[shard_num])
        return results

    def __getattr__(self, func_name):
        def func(*args):
            if (func_name in self.disallowed_sharded_operations):
                raise Exception("Cannot call '%s' on sharded Redis".format(
                    func_name))
            key = args[0]
            shard = self.get_shard_by_key(key)
            return getattr(shard, func_name)(*args)
        return func
