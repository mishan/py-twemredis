import twemredis


def main():
    tr = twemredis.TwemRedis("example.yml")
    key = tr.get_canonical_key('canceled', '5430406808')
    print(tr.zrange(key, 0, -1))

if __name__ == "__main__":
    main()
