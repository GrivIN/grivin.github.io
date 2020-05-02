---
layout: post
title: How to setup custom cassandra schema within Docker (-compose)
date: 2020-05-02 11:04
category:
author:
tags: []
summary:
---

Often we use the docker and docker-compose systems to speed up our development and testing setup. It is great when with just one command `docker-compose up` we can start whole env locally. It works great with SQL databases but when we use cassandra database there is no way to create initial database the same way as we used to do with other docker images. I mean with putting .sql files into some folder and just mapping it into `docker-entrypoint-initdb.d` folder inside the container like:

``` yml
# ./docker-compose.yml
version: "3.7"
services:
 cassandra:
    image: cassandra:latest
    volumes:
      - ./fixtures/cassandra:/docker-entrypoint-initdb.d
```
Unfortunately when we just put a .cql (lets use an example file to create an user table from below) script into `./fixtures/cassandra` nothing happens.

``` sql
-- ./fixtures/cassandra/0001-schema.cql
CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND durable_writes = true;

CREATE TABLE IF NOT EXISTS test.user_accounts_by_country_gender (
    city text,
    state text,
    zip_code text,
    country text,
    joined_at timestamp,
    joined_day int,
    joined_month int,
    joined_year int,
    first_name text,
    last_name text,
    display_name text,
    gender text,
    email text,
    subscription text,
    id uuid,
    PRIMARY KEY ((country, gender), email)
) WITH CLUSTERING ORDER BY (email ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';
```

Let's start with change in our docker-compose file, we will need a custom `Dockerfile` for our modified cassandra image, we can call it `Dockerfile-cassandra` and set as cassandra service build source instead an image.

``` yml
# ./docker-compose.yml
version: "3.7"
services:
 cassandra:
    build:
      context: .
      dockerfile: Dockerfile-cassandra
    restart: always
    ports:
      - 7000:7000
      - 7001:7001
      - 7199:7199
      - 9042:9042
      - 9160:9160
    volumes:
      - ./fixtures/cassandra:/docker-entrypoint-initdb.d
    networks:
      - local
    environment:
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_RACK=rack1
      - CASSANDRA_ENDPOINT_SNITCH="GossipingPropertyFileSnitch"
networks:
  local:
    name: blog-tests
```

In our custom `Dockerfile` we will wrap cassandra entry point with an scrip that will scan `docker-entrypoint-initdb.d/` directory and execute each sql file in it (also will sleep 10 seconds if script fails assuming that cassandra cluster is not ready)

``` bash
# ./scripts/cassandra-entrypoint-wrap.sh
#!/bin/bash

for f in docker-entrypoint-initdb.d/*; do
    case "$f" in
        *.cql)    echo "$0: running $f" &&
            until cqlsh -f "$f"; do
                >&2 echo "Unavailable: sleeping";
                sleep 10;
            done & ;;
    esac
    echo
done
echo "Migration done, starting server"

exec usr/local/bin/docker-entrypoint.sh "$@"
```

And here is mentioned docker file itself> We need to copy our wrapper script into /bin folder and make it executable. Change od entry point is also required

``` dockerfile
# ./Dockerfile-cassandra
FROM cassandra:latest

COPY scripts/cassandra-entrypoint-wrap.sh /bin/entrypoint-wrap

RUN chmod a+x /bin/entrypoint-wrap
ENTRYPOINT ["entrypoint-wrap"]
CMD ["cassandra", "-f"]
```

Now, as everything is ready, we can bring the environment up.  This should start the cassandra server in out test network and will be accessible via localhost due to network mapping that we set up earlier.

``` bash
docker-compose up
```
