Shard - consistent workers
==========================

An Erlang library to distribute work to worker processes consistently, the
default implementation uses riak core's chash hashing module to hash the key to
a worker.

Usage
-----

You create a hash by calling::

    {ok, Shard} = shard:start_link([{rscbag_opts, ResourceOpts}, {hash_fun, HashFun}]),

where rscbag_opts are the options that will be passed to an instance of
`rscbag <https://github.com/marianoguerra/rscbag>`_ and hash_fun is a callable
that will receive a Key and will return it's hash, we provide a default
implementation on shard_util using riak_core's chash.

Optionally you can pass {shard_opts, PropList} to start_link which can contain
options to pass to each shard as it's created, this options also will contain
an entry {shard_lib_partition, Partition} which contains the partition
identifying the shard.

You can create a chash hash_fun with 64 vnodes like this::

    HashFun = shard_util:new_chash_fun(64, Name).

Now you can ask the shard to handle the call for you by calling shard:handle::

    shard:handle(Shard, Key, Callable).

the module will hash the key with hash_fun, lookup the shard for that key if it
exists it will call Callable otherwise it will create the shard and call
Callable.

a Callable can be one of:

* pid: message will be sent to pid
* fun: will be called passing message as first argument
* {Module, Function}: will be called passing message as first argument
* {Module, Function, Args}: will be called passing message prepended to Args

License
-------

MPL 2.0, see LICENSE file for details
