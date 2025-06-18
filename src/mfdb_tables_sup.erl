%%% @copyright 2020 Leonard Boyce <leonard.boyce@lucidlayer.com>
%%% @hidden
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%%% use this file except in compliance with the License. You may obtain a copy of
%%% the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%%% License for the specific language governing permissions and limitations under
%%% the License.
-module(mfdb_tables_sup).

-behaviour(supervisor).

%% API
-export([
    add/1,
    remove/1
]).

%% Supervisor
-export([start_link/0]).
-export([init/1]).

-include("mfdb.hrl").

-spec add(table_name()) -> ok.
add(Table) ->
    Id = {mfdb_table_sup, Table},
    Spec =
        {Id, {mfdb_table_sup, start_link, [Table]}, transient, 5000, supervisor, [mfdb_table_sup]},
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
    io:format("Removing table ~p~n", [Table]),
    Id = {mfdb_table_sup, Table},
    Term = supervisor:terminate_child(?MODULE, Id),
    Del = supervisor:delete_child(?MODULE, Id),
    io:format("Removing table ~p~nT:~p~nD:~p~n", [Table, Term, Del]),
    ok.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 5, 10}, []}}.
