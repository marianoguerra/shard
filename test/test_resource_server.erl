-module(test_resource_server).

-export([ping/1, ping/2, accum/2, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).


-behaviour(gen_server).

-record(state, {partition, items=[]}).

ping(Pid) ->
    ping(Pid, make_ref()).

ping(Pid, Ref) ->
    gen_server:call(Pid, {ping, Ref}).

accum(Pid, Value) ->
    gen_server:call(Pid, {accum, Value}).

stop(Pid) ->
    gen_server:call(Pid, stop).

%% gen_server callbacks


init(Opts) ->
    {shard_lib_partition, Partition} = proplists:lookup(shard_lib_partition, Opts),
    State = #state{partition=Partition},
    {ok, State}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call({ping, Ref}, _From, State=#state{partition=Partition}) ->
    {reply, {pong, Ref, Partition}, State};

handle_call({accum, Value}, _From, State=#state{items=Items}) ->
    NewItems = [Value|Items],
    {reply, {ok, NewItems}, State#state{items=NewItems}}.

handle_info(_Msg, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

