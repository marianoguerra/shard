-module(shard).
-behaviour(gen_server).

-export([start_link/1, handle/3, handle_all/2, handle_existing/3, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-ignore_xref([start_link/1, handle/3, handle_all/2, handle_existing/3, stop/1]).

-record(state, {resources, hash_fun}).

-type shard() :: pid().
-type state() :: #state{}.
-type key() :: term().
-type start_error() :: {already_started, pid()} | term().
-type opts() :: [opt()].
-type opt() :: {resource_opts, atom()} |
               {hash_fun, callable()}.
-type mf() :: {atom(), atom()}.
-type callable() :: mf() | mfa() | pid() | fun().

%% API

-spec start_link(opts()) -> {ok, pid()} | ignore | {error, start_error()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec handle(shard(), key(), callable()) -> term().
handle(Ref, Key, Handler) ->
    gen_server:call(Ref, {handle, Key, Handler}).

handle_all(Ref, Handler) ->
    gen_server:call(Ref, {handle_all, Handler}).

-spec handle_existing(shard(), key(), callable()) -> term().
handle_existing(Ref, Key, Handler) ->
    gen_server:call(Ref, {handle_existing, Key, Handler}).

stop(Ref) ->
    gen_server:call(Ref, stop).

%% gen_server callbacks

-spec init(opts()) -> {ok, state()}.
init(Opts) ->
    {resource_opts, RscOpts} = proplists:lookup(resource_opts, Opts),
    {hash_fun, HashFun} = proplists:lookup(hash_fun, Opts),
    {ok, Resources} = rscbag:init(RscOpts),
    State = #state{resources=Resources, hash_fun=HashFun},
    {ok, State}.

-spec handle_call({handle, key(), callable()}, any(), state()) -> {reply, term(), state()}.
handle_call({handle, Key, Handler}, _From,
            State=#state{resources=Resources, hash_fun=HashFun}) ->
    Partition = call_handler({hash, Key}, HashFun),
    {Reply, NewResources} = call_handler_1(Partition, Resources, Handler),
    NewState = State#state{resources=NewResources},
    {reply, Reply, NewState};

handle_call({handle_all, Handler}, _From,
            State=#state{resources=Resources, hash_fun=HashFun}) ->
    Partitions = call_handler(all, HashFun),
    Fun = fun (Partition, {RepliesIn, ResourcesIn}) ->
                  {Reply, ResourcesOut} = call_handler_1(Partition, ResourcesIn, Handler),
                  {[{Partition, Reply}|RepliesIn], ResourcesOut}
          end,
    {Reply, NewResources} = lists:foldl(Fun, {[], Resources}, Partitions),
    NewState = State#state{resources=NewResources},
    {reply, Reply, NewState};

handle_call({handle_existing, Key, Handler}, _From,
            State=#state{resources=Resources, hash_fun=HashFun}) ->
    Partition = call_handler({hash, Key}, HashFun),
    R = case rscbag:get_existing(Resources, Partition) of
            {{ok, found, Pid}, NResources} ->
                call_handler(Partition, Pid, Handler, NResources);
            {{error, notfound}, _NResources}=E -> E
        end,
    {Reply, NewResources} = R,
    NewState = State#state{resources=NewResources},
    {reply, Reply, NewState};

handle_call(stop, _From, State=#state{resources=Resources}) ->
    ok = rscbag:stop(Resources),
    {stop, normal, stopped, State}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Info},
            State=#state{resources=Resources}) ->
    R = case rscbag:remove_by_val(Resources, Pid, false) of
            {ok, NResources} -> NResources;
            {{error, notfound}, NResources} -> NResources
        end,

    NewResources = R,
    NewState = State#state{resources=NewResources},
    {noreply, NewState};

handle_info(_Msg, State) ->
    %lager:warning("Unexpected handle info message: ~p~n", [Msg]),
    {noreply, State}.

handle_cast(_Msg, State) ->
    %lager:warning("Unexpected handle cast message: ~p~n", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% Internals

call_handler(Pid, Handler) when is_pid(Handler) ->
    erlang:send(Handler, Pid);
call_handler(Pid, Handler) when is_function(Handler) ->
    Handler(Pid);
call_handler(Pid, {M, F}) ->
    erlang:apply(M, F, [Pid]);
call_handler(Pid, {M, F, A}) ->
    erlang:apply(M, F, [Pid|A]).

call_handler(Partition, Pid, Handler, Resources) ->
    case is_process_alive(Pid) of
        true ->
            Result = call_handler(Pid, Handler),
            {Result, Resources};
        false ->
            {_, NResources} = rscbag:remove_by_val(Resources, Pid),
            call_handler_1(Partition, NResources, Handler)
    end.

call_handler_1(Partition, Resources, Handler) ->
    ResourceInitOpts = [{shard_lib_partition, Partition}],
    case rscbag:get(Resources, Partition, ResourceInitOpts) of
        {{ok, found, Pid}, NResources} ->
            call_handler(Partition, Pid, Handler, NResources);
        {{ok, created, Pid}, NResources} ->
            erlang:monitor(process, Pid),
            call_handler(Partition, Pid, Handler, NResources);
        {{error, _Reason}, _NResources}=E -> E
    end.

