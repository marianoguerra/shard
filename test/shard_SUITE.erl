-module(shard_SUITE).
-compile(export_all).

all() -> [noop, ping_mfa].

init_per_suite(Config) -> 
    Config.

init_per_testcase(Test, Config) ->
    ResourceOpts = [{resource_handler, test_resource_handler}],
    HashFun = shard_util:new_chash_fun(64, Test),
    {ok, Shard} = shard:start_link([{resource_opts, ResourceOpts}, {hash_fun, HashFun}]),
    [{shard, Shard}|Config].

end_per_testcase(_Test, Config) ->
    Shard = shard(Config),
    stopped = shard:stop(Shard),
    Config.

shard(Config) -> proplists:get_value(shard, Config).

noop(_) ->
    ok.

ping_mfa(Config) ->
    Shard = shard(Config),
    MFA = {test_resource_server, ping, [ref1]},
    {pong, ref1, Partition} = shard:handle(Shard, <<"mariano">>, MFA),
    ct:pal("Partition ~p", [Partition]).

