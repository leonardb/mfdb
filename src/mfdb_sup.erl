-module(mfdb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    %% This is a bit of a kludge since mfdb may be included by
    %% another application but not actually have a cluster defined
    Children = case application:get_env(mfdb, cluster, undefined) of
                   undefined ->
                       [];
                   _ ->
                       [?CHILD(mfdb_table_sup, supervisor),
                        ?CHILD(mfdb_watcher_sup, supervisor),
                        ?CHILD(mfdb_manager, worker)]
               end,
    {ok, { {one_for_all, 5, 10}, Children} }.
