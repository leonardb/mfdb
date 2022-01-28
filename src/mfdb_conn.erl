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
-module(mfdb_conn).

%% API
-export([init/0,
         connection/0,
         connection/1]).

-include("mfdb.hrl").

-spec init() -> ok.
init() ->
    case application:get_all_env(mfdb) of
        [] ->
            ok;
        Settings ->
            %% Crash if missing app_key or cluster
            case lists:keyfind(app_key, 1, Settings) of
                false ->
                    {error, missing_app_key};
                {app_key, AppKey} ->
                    case lists:keyfind(cluster, 1, Settings) of
                        false ->
                            {error, missing_cluster_definition};
                        {cluster, Cluster} ->
                            Conn = #conn{
                                      key           = AppKey,
                                      cluster       = Cluster,
                                      tls_key_path  = proplists:get_value(tls_key_path, Settings, undefined),
                                      tls_cert_path = proplists:get_value(tls_cert_path, Settings, undefined),
                                      tls_ca_path   = proplists:get_value(tls_ca_path, Settings, undefined)
                                     },
                            ok = persistent_term:put(mfdb_conn, Conn),
                            ok = load_fdb_nif_(Conn)
                    end
            end
    end.

%% Load the NIF (try and ensure it's only loaded only once)
%% There must be a better way of checking if it's been initialized
load_fdb_nif_(#conn{tls_key_path = undefined}) ->
    try
        erlfdb_nif:init(),
        ok
    catch error:{reload, _} ->
            io:format("NIF already loaded~n"),
            ok
    end;
load_fdb_nif_(#conn{tls_key_path = KeyPath, tls_cert_path = CertPath, tls_ca_path = CAPath}) ->
    FdbNetworkOptions = [{tls_ca_bytes, fread(CAPath)},
                         {tls_key_bytes, fread(KeyPath)},
                         {tls_cert_bytes, fread(CertPath)}],
    try
        ok = erlfdb_nif:init_manual(FdbNetworkOptions),
        ok
    catch
        error:{reload, _} ->
            io:format("NIF already loaded~n"),
            ok
    end.

fread(Path) when is_binary(Path) ->
    fread(binary_to_list(Path));
fread(Path) when is_list(Path) ->
    {ok, Data} = file:read_file(Path),
    Data.

%% @doc Open and return an erlfdb database connection
-spec connection() -> fdb_db().
connection() ->
    #conn{} = Conn = persistent_term:get(mfdb_conn),
    connection(Conn).

-spec connection(#conn{}) -> fdb_db().
connection(#conn{cluster = Cluster} = _Conn) ->
    %% ok = load_fdb_nif_(Conn),
    {erlfdb_database, _} = Db = erlfdb:open(Cluster),
    Db.
