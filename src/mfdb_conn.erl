%%%-------------------------------------------------------------------
%%% @copyright (C) 2020, Leonard Boyce
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
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
                            ets:insert(mfdb_manager, Conn),
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
    {ok, CABytes} = file:read_file(binary_to_list(CAPath)),
    FdbNetworkOptions = [{tls_ca_bytes, CABytes},
                         {tls_key_path, KeyPath},
                         {tls_cert_path, CertPath}],
    try
        erlfdb_nif:init(FdbNetworkOptions),
        ok
    catch
        error:{reload, _} ->
            io:format("NIF already loaded~n"),
            ok
    end.

%% @doc Open and return an erlfdb database connection
-spec connection() -> db().
connection() ->
    [#conn{} = Conn] = ets:lookup(mfdb_manager, conn),
    connection(Conn).

-spec connection(#conn{}) -> db().
connection(#conn{cluster = Cluster} = Conn) ->
    ok = load_fdb_nif_(Conn),
    {erlfdb_database, _} = Db = erlfdb:open(Cluster),
    Db.
