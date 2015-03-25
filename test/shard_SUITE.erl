-module(shard_SUITE).
-compile(export_all).

all() -> [noop, stop_shard, ping_mfa, ping_mf, ping_fun, accum_sharded, handle_existing,
         accum_stop_accum, two_keys_same_hash, ping_all].

init_per_suite(Config) -> 
    Config.

init_per_testcase(Test, Config) ->
    ResourceOpts = [{resource_handler, test_resource_handler}],
    HashFun = shard_util:new_chash_fun(64, Test),
    {ok, Shard} = shard:start_link([{rscbag_opts, ResourceOpts}, {hash_fun, HashFun}]),
    [{shard, Shard}|Config].

end_per_testcase(_Test, Config) ->
    Config.

shard(Config) -> proplists:get_value(shard, Config).

noop(_) ->
    ok.

ping_mfa(Config) ->
    Shard = shard(Config),
    MFA = {test_resource_server, ping, [ref1]},
    {pong, ref1, Partition} = shard:handle(Shard, <<"mariano">>, MFA),
    ct:pal("Partition ~p", [Partition]).

ping_mf(Config) ->
    Shard = shard(Config),
    MFA = {test_resource_server, ping},
    {pong, Ref, Partition} = shard:handle(Shard, <<"mariano">>, MFA),
    ct:pal("Partition ~p Ref ~p", [Partition, Ref]).

ping_all(Config) ->
    Shard = shard(Config),
    MFA = {test_resource_server, ping},
    Replies = shard:handle_all(Shard, MFA),
    64 = length(Replies),
    true = lists:all(fun ({Partition, {pong, _, Partition}}) -> true; (_) -> false end,
                     Replies).

ping_fun(Config) ->
    Shard = shard(Config),
    Fun = fun (Pid) ->
                  test_resource_server:ping(Pid, ref2)
          end,
    {pong, ref2, Partition} = shard:handle(Shard, <<"mariano">>, Fun),
    ct:pal("Partition ~p", [Partition]).

accum_sharded(Config) ->
    Shard = shard(Config),
    Accum = fun (Key, Value) ->
                    MFA = {test_resource_server, accum, [Value]},
                    shard:handle(Shard, Key, MFA)
            end,
    {ok, [a]} = Accum(<<"bob">>, a),
    {ok, [b, a]} = Accum(<<"bob">>, b),
    {ok, [1]} = Accum(<<"patrick">>, 1),
    {ok, [2, 1]} = Accum(<<"patrick">>, 2),
    {ok, [c, b, a]} = Accum(<<"bob">>, c),
    {ok, [3, 2, 1]} = Accum(<<"patrick">>, 3).

handle_existing(Config) ->
    Shard = shard(Config),
    MFA = {test_resource_server, ping, [ref1]},
    {pong, ref1, Partition} = shard:handle(Shard, <<"mariano">>, MFA),
    {pong, ref1, Partition} = shard:handle_existing(Shard, <<"mariano">>, MFA),
    {error, notfound} = shard:handle_existing(Shard, <<"patrick">>, MFA).

accum_stop_accum(Config) ->
    Shard = shard(Config),
    Accum = fun (Key, Value) ->
                    MFA = {test_resource_server, accum, [Value]},
                    shard:handle(Shard, Key, MFA)
            end,
    Stop = fun (Key) ->
                    MF = {test_resource_server, stop},
                    shard:handle_existing(Shard, Key, MF)
            end,
    {ok, [a]} = Accum(<<"bob">>, a),
    {ok, [b, a]} = Accum(<<"bob">>, b),
    {ok, [1]} = Accum(<<"patrick">>, 1),
    {ok, [2, 1]} = Accum(<<"patrick">>, 2),
    {ok, [c, b, a]} = Accum(<<"bob">>, c),
    stopped = Stop(<<"bob">>),
    {ok, [3, 2, 1]} = Accum(<<"patrick">>, 3),
    stopped = Stop(<<"patrick">>),

    {ok, [a]} = Accum(<<"bob">>, a),
    {ok, [b, a]} = Accum(<<"bob">>, b),
    {ok, [1]} = Accum(<<"patrick">>, 1),
    {ok, [2, 1]} = Accum(<<"patrick">>, 2),
    {ok, [c, b, a]} = Accum(<<"bob">>, c),
    {ok, [3, 2, 1]} = Accum(<<"patrick">>, 3).

stop_shard(Config) ->
    Shard = shard(Config),
    stopped = shard:stop(Shard).

two_keys_same_hash(Config) ->
    Shard = shard(Config),
    Accum = fun (Key, Value) ->
                    MFA = {test_resource_server, accum, [Value]},
                    shard:handle(Shard, Key, MFA)
            end,
    {ok, [a]} = Accum(<<"sandy2">>, a),
    {ok, [b, a]} = Accum(<<"sandy5">>, b),
    {ok, [c, b, a]} = Accum(<<"sandy2">>, c).

