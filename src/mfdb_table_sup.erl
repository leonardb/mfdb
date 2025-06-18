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
-module(mfdb_table_sup).

-behaviour(supervisor).

%% Supervisor
-export([start_link/1]).
-export([init/1]).

start_link(Table) ->
    supervisor:start_link(?MODULE, [Table]).

init([Table]) ->
    TableSpec = {{mfdb, Table}, {mfdb, start_link, [Table]}, transient, 5000, worker, [mfdb]},
    ReaperSpec = {
        {mfdb_reaper, Table}, {mfdb_reaper, start_link, [Table]}, transient, 5000, worker, [
            mfdb_reaper
        ]
    },
    {ok, {{one_for_all, 5, 10}, [TableSpec, ReaperSpec]}}.
