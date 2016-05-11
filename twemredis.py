"""
twemproxy/nutcracker sharded Redis library for Python.
- Capable of dealing with twemproxy sharded Redis configuration schemes.
- Talks to the Sentinels to obtain the Redis shards
"""

import collections
import hashlib
import re

from redis.sentinel import Sentinel
import yaml

__all__ = ['TwemRedis']


class TwemRedis:
    """
    A redis wrapper library for using twemproxy sharded Redis.
    Allows running operations on individual shards or across all shards.

    Attributes:
        disallowed_sharded_operations:
            These Redis functions cannot be called indirectly on a TwemRedis
            instance. If they are to be used, then the operations must be
            performed on the shards directly.
    """

    disallowed_sharded_operations = ['hscan', 'scan', 'sscan', 'zscan']

    def __init__(self, config_file):
        self._config = self._parse_config(config_file)

        self._shard_name_format = self._config['shard_name_format']

        self._hash_tag = self._config['hash_tag']
        self._hash_start = self._hash_tag[0]
        self._hash_stop = self._hash_tag[1]

        if 'sentinels' in self._config:
            self._sentinels = self._config['sentinels']
            self._num_shards = int(self._config['num_shards'])
            self._masters = None
        elif 'masters' in self._config:
            self._masters = self._config['masters']
            self._num_shards = len(self._masters)
            self._sentinels = None

        if 'timeout' in self._config:
            self._timeout = self._config['timeout']
        else:
            self._timeout = 2.0

        self._canonical_keys = self.compute_canonical_key_ids()

        self._init_redis_shards()

    def _parse_config(self, config_file):
        """
        _parse_config takes a path to a yml file and returns the parsed
        object representation of the file. This is a convenient method
        to override in unit tests.
        """
        return yaml.load(open(config_file, 'r'))

    def _init_redis_shards(self):
        """
        init_redis_shards is used internally to connect to the Redis sentinels
        and populate self.shards with the redis.StrictRedis instances. This
        is a convenient method to override / stub out in unit tests.
        """
        self._shards = {}
        if self._sentinels is not None:
            self.init_shards_from_sentinel()
        elif self._masters is not None:
            self.init_shards_from_masters()
        else:
            raise Exception("You must either specify sentinels or masters")

    def init_shards_from_sentinel(self):
        sentinel_client = Sentinel(
            [(h, 8422) for h in self._sentinels], socket_timeout=self._timeout)
        # Connect to all the shards with the names specified per
        # shard_name_format. The names are important since it's getting
        # the instances from Redis Sentinel.
        for shard_num in range(0, self.num_shards()):
            shard_name = self.get_shard_name(shard_num)
            self._shards[shard_num] = sentinel_client.master_for(
                shard_name, socket_timeout=self._timeout)
        # Just in case we need it later.
        self._sentinel_client = sentinel_client

    def init_shards_from_masters(self):
        for shard_num in range(0, self.num_shards()):
            master = self._masters[shard_num]
            (host, port) = master.split(' ')
            shard = redis.StrictRedis(host, port,
                                      socket_timeout=self._timeout)
            self._shards[shard_num] = shard

    def num_shards(self):
        """
        num_shards returns the number of Redis shards in this cluster.
        """
        return self._num_shards

    def get_shard_name(self, shard_num):
        """
        get_shard_name returns the name of the shard given its number.
        It uses the 'shard_name_format' from the configuration file
        to format the name from the number.
        Keyword arguments:
        shard_num - The shard number (e.g. 0)

        Returns the shard name (e.g. tdb001)
        """
        return self._shard_name_format.format(shard_num)

    def get_shard_names(self):
        """
        get_shard_names returns an array containing the names of the shards
        in the cluster. This is determined with num_shards and
        shard_name_format
        """
        results = []
        for shard_num in range(0, self.num_shards()):
            shard_name = self.get_shard_name(shard_num)
            results.append(shard_name)

        return results

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
        shard_num = self.get_shard_num_by_key_id(key_id)
        return self.get_shard_by_num(shard_num)

    def get_shard_num_by_key_id(self, key_id):
        """
        get_shard_num_by_key_id returns the Redis shard number (zero-indexed)
        given a key id.
        Keyword arguments:
        key_id -- the key id (e.g. '12345' or 'anythingcangohere')

        This method is critical in how the Redis cluster sharding works. We
        emulate twemproxy's md5 distribution algorithm.
        """
        # TODO: support other hash functions?
        m = hashlib.md5(str(key_id).encode('ascii')).hexdigest()
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
        is useful for sets that need to exist on all shards. See
        compute_canonical_key_ids for how these are calculated.

        Keyword arguments:
        key_type -- the type of key (e.g. 'canceled')
        key_id -- the key id (e.g. '12345')

        returns the canonical key string (e.g. 'canceled:{12}')
        """
        canonical_key_id = self.get_canonical_key_id(key_id)
        return self.get_key(key_type, canonical_key_id)

    def get_canonical_key_id(self, key_id):
        """
        get_canonical_key_id is used by get_canonical_key, see the comment
        for that method for more explanation.

        Keyword arguments:
        key_id -- the key id (e.g. '12345')

        returns the canonical key id (e.g. '12')
        """
        shard_num = self.get_shard_num_by_key_id(key_id)
        return self._canonical_keys[shard_num]

    def get_canonical_key_id_for_shard(self, shard_num):
        """
        get_canonical_key_id_for_shard is a utility method. It will
        return the canonical key id for a given shard number.

        Keyword arguments:
        shard_num -- the shard number (e.g. 0)

        returns the canonical key id (e.g. '12')
        """
        return self._canonical_keys[shard_num]

    def get_shard_by_num(self, shard_num):
        """
        get_shard_by_num returns the shard at index shard_num.

        Keyword arguments:
        shard_num -- The shard index

        Returns a redis.StrictRedis connection or raises a ValueError.
        """
        if shard_num < 0 or shard_num >= self.num_shards():
            raise ValueError("requested invalid shard# {0}".format(shard_num))

        return self._shards[shard_num]

    def _get_key_id_from_key(self, key):
        """
        _get_key_id_from_key returns the key id from a key, if found. otherwise
        it just returns the key to be used as the key id.

        Keyword arguments:
        key -- The key to derive the ID from. If curly braces are found in the
               key, then the contents of the curly braces are used as the
               key id for the key.

        Returns the key id portion of the key, or the whole key if no hash
        tags are present.
        """
        key_id = key

        regex = '{0}([^{1}]*){2}'.format(self._hash_start, self._hash_stop,
                                         self._hash_stop)
        m = re.search(regex, key)
        if m is not None:
            # Use what's inside the hash tags as the key id, if present.
            # Otherwise the whole key will be used as the key id.
            key_id = m.group(1)

        return key_id

    def compute_canonical_key_ids(self, search_amplifier=100):
        """
        A canonical key id is the lowest integer key id that maps to
        a particular shard. The mapping to canonical key ids depends on the
        number of shards.

        Returns a dictionary mapping from shard number to canonical key id.

        This method will throw an exception if it fails to compute all of
        the canonical key ids.
        """
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
                                 num_shards, len(canonical_keys),
                                 search_amplifier))

        return canonical_keys

    def execute_on_all_shards(self, closure):
        results = {}
        for shard_num in range(0, self.num_shards()):
            shard = self.get_shard_by_num(shard_num)
            results[shard_num] = closure(shard)
        return results

    def run_on_all_shards(self, func, *args, **kwargs):
        results = {}
        for shard_num in range(0, self.num_shards()):
            shard = self.get_shard_by_num(shard_num)
            method = getattr(shard, func)
            if method is not None:
                results[shard_num] = getattr(shard, func)(*args, **kwargs)
            else:
                raise Exception("undefined method '%s' on shard %d".format(
                    method, shard_num))
        return results

    def keys(self, args):
        """
        keys wrapper that queries every shard. This is an expensive
        operation.

        This method should be invoked on a TwemRedis instance as if it
        were being invoked directly on a StrictRedis instance.
        """
        results = {}
        # TODO: parallelize
        for shard_num in range(0, self.num_shards()):
            shard = self.get_shard_by_num(shard_num)
            results[shard_num] = shard.keys(args)
        return results

    def mget(self, args):
        """
        mget wrapper that batches keys per shard and execute as few
        mgets as necessary to fetch the keys from all the shards involved.

        This method should be invoked on a TwemRedis instance as if it
        were being invoked directly on a StrictRedis instance.
        """
        key_map = collections.defaultdict(list)
        results = {}
        for key in args:
            shard_num = self.get_shard_num_by_key(key)
            key_map[shard_num].append(key)

        # TODO: parallelize
        for shard_num in key_map.keys():
            shard = self.get_shard_by_num(shard_num)
            results[shard_num] = shard.mget(key_map[shard_num])
        return results

    def mset(self, args):
        """
        mset wrapper that batches keys per shard and execute as few
        msets as necessary to set the keys in all the shards involved.

        This method should be invoked on a TwemRedis instance as if it
        were being invoked directly on a StrictRedis instance.
        """
        key_map = collections.defaultdict(dict)
        result_count = 0
        for key in args.keys():
            value = args[key]
            shard_num = self.get_shard_num_by_key(key)
            key_map[shard_num][key] = value

        # TODO: parallelize
        for shard_num in key_map.keys():
            shard = self.get_shard_by_num(shard_num)
            result_count += shard.mset(key_map[shard_num])

        return result_count

    def __getattr__(self, func_name):
        """
        Allow directly calling StrictRedis operations on a TwemRedis
        instance. This will take the key and apply it to the appropriate
        shard. Certain operations like KEYS and MGET are supported but are
        handled in their own wrapper methods.
        """
        def func(key, *args, **kwargs):
            if (func_name in self.disallowed_sharded_operations):
                raise Exception("Cannot call '%s' on sharded Redis".format(
                    func_name))

            shard = self.get_shard_by_key(key)
            return getattr(shard, func_name)(key, *args, **kwargs)
        return func
