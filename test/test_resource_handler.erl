-module(test_resource_handler).
-export([init/1, stop/1]).

-behaviour(rscbag_resource_handler).

% rscbag_resource_handler callbacks

init(Opts) ->
    gen_server:start_link(test_resource_server, Opts, []).

stop(Pid) ->
    test_resource_handler:stop(Pid).

