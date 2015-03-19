-module(shard_util).
-export([new_chash_fun/2, chash_fun/2]).

-ignore_xref([new_chash_fun/2, chash_fun/2]).

-record(chash_state, {chash, chashbin}).

new_chash_fun(NumPartitions, SeedNode) ->
    CH = chash:fresh(NumPartitions, SeedNode),
    CHB = chashbin:create(CH),
    State = #chash_state{chash=CH, chashbin=CHB},
    Args = [State],
    {shard_util, chash_fun, Args}.

chash_fun({hash, Key}, #chash_state{chashbin=CHB}) ->
    DocIdx = chash:key_of(Key),
    Itr = chashbin:iterator(DocIdx, CHB),
    N = 1,
    {[{Partition, _}], _} = chashbin:itr_pop(N, Itr),
    Partition;

chash_fun(all, #chash_state{chash=CH}) ->
    Nodes = chash:nodes(CH),
    lists:map(fun  ({Partition, _}) -> Partition end, Nodes).

