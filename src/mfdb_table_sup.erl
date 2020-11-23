-module(mfdb_table_sup).

-behaviour(supervisor).

%% API
-export([add/1,
         remove/1]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("mfdb.hrl").

-spec add(table_name()) -> ok.
add(Table) ->
    Id = {mfdb, Table},
    Spec = {Id,
            {mfdb, start_link, [Table]},
            transient,
            5000,
            worker,
            [mfdb]},
    case supervisor:start_child(?MODULE, Spec) of
        {ok, _Pid} ->
            ok;
        {ok, _Pid, _Msg} ->
            ok;
        {error, already_present} ->
            ok = supervisor:delete_child(?MODULE, Id),
            add(Table);
        {error, {already_started, Pid}} ->
            case is_process_alive(Pid) of
                true ->
                    ok;
                false ->
                    ok = supervisor:terminate_child(?MODULE, Id),
                    ok = supervisor:delete_child(?MODULE, Id),
                    add(Table)
            end
    end.

-spec remove(table_name()) -> ok.
remove(Table) ->
    Id = {mfdb, Table},
    catch supervisor:terminate_child(?MODULE, Id),
    catch supervisor:delete_child(?MODULE, Id),
    ok.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, { {one_for_one, 5, 10}, []} }.
