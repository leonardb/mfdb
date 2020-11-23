%%%-------------------------------------------------------------------
%%% @copyright (C) 2020, Leonard Boyce
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(mfdb_watcher_sup).

-behaviour(supervisor).

-export([create/3]).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.

-spec create(Table :: binary(), Prefix :: binary(), Key :: binary()) -> ok.
create(Table, Prefix, Key) ->
    Name = {mfdb_watcher, {Table, Prefix, Key}},
    Spec = {Name,
            {mfdb_watcher, start_link, [Table, Prefix, Key]},
            transient,
            5000,
            worker,
            [mfdb_watcher]},
    case supervisor:start_child(?MODULE, Spec) of
        {ok, _} ->
            ok;
        {ok, _, _} ->
            ok;
        {error, already_present} ->
            supervisor:terminate_child(?MODULE, Name),
            supervisor:delete_child(?MODULE, Name),
            create(Table, Prefix, Key);
        {error, {already_started, Pid}} ->
            case is_process_alive(Pid) of
                true ->
                    ok;
                false ->
                    supervisor:terminate_child(?MODULE, Name),
                    supervisor:delete_child(?MODULE, Name),
                    create(Table, Prefix, Key)
            end
    end.
