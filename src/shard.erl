-module(shard).
-behaviour(gen_server).

-export([start_link/1, handle/3]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-ignore_xref([start_link/1, handle/3]).

-record(state, {resources, hash_fun}).

-type shard() :: pid().
-type state() :: #state{}.
-type key() :: term().
-type start_error() :: {already_started, pid()} | term().
-type opts() :: [opt()].
-type opt() :: {resource_handler, atom()} |
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
    Partition = handle(Key, HashFun),
    ResourceInitOpts = [{shard_lib_partition, Partition}],
    R = case rscbag:get(Resources, Partition, ResourceInitOpts) of
            {{ok, found, Pid}, NResources} ->
                Result = handle(Pid, Handler),
                {Result, NResources};
            {{ok, created, Pid}, NResources} ->
                erlang:monitor(process, Pid),
                Result = handle(Pid, Handler),
                {Result, NResources};
            {error, Reason, NResources} ->
                {{error, Reason}, NResources}
        end,
    {Reply, NewResources} = R,
    NewState = State#state{resources=NewResources},
    {reply, Reply, NewState}.

handle_info({'DOWN', _MonitorRef, process, Pid, _Info},
            State=#state{resources=Resources}) ->
    R = case rscbag:remove_by_val(Resources, Pid) of
            {ok, NResources} -> NResources;
            {error, notfound, NResources} -> NResources
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

handle(Msg, Pid) when is_pid(Pid) ->
    erlang:send(Pid, Msg);
handle(Msg, Fun) when is_function(Fun) ->
    Fun(Msg);
handle(Msg, {M, F}) ->
    erlang:apply(M, F, [Msg]);
handle(Msg, {M, F, A}) ->
    erlang:apply(M, F, [Msg|A]).
