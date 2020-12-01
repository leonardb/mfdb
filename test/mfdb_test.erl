-module(mfdb_test).

-include_lib("eunit/include/eunit.hrl").

-record(test_a, {id :: integer(), value :: binary()}).
-define(TEST_A, {test_a, [{id, integer}, {value, binary}]}).
-define(APP_KEY, <<"cb136a3d-de40-4a85-bd10-bb23f6f1ec2a">>).

aaa_test() ->
    {ok, ClusterFile} = init_test_cluster_int([]),
    ?assert(is_binary(ClusterFile)),
    application:ensure_all_started(mfdb),
    ok.

zzz_test() ->
    %% Delete everything from the test database
    Db = mfdb_conn:connection(),
    erlfdb:clear_range(Db, <<>>, <<16#FF>>).

aa_create_test() ->
    Ok = mfdb:create_table(test_a, [{record, ?TEST_A}]),
    ?assertEqual(ok, Ok).

bb_insert_test() ->
    [mfdb:insert(test_a, #test_a{id = X, value = integer_to_binary(X, 32)}) || X <- lists:seq(1, 50)],
    {ok, Count} = mfdb:table_info(test_a, count),
    ?assertEqual(50, Count).

cc_count_test() ->
    Rec50In = #test_a{id = 50, value = integer_to_binary(50, 32)},
    {ok, Rec50Out} = mfdb:lookup(test_a, 50),
    ?assertEqual(Rec50In, Rec50Out).

dd_value_test() ->
    IdSumIn = lists:sum([X || X <- lists:seq(1, 50)]),
    IdSumOut = mfdb:fold(test_a, fun(#test_a{id = X}, Acc) -> Acc + X end, 0),
    ?assertEqual(IdSumIn, IdSumOut).

ee_counter_test() ->
    ?assertEqual(50, mfdb:update_counter(test_a, my_counter, 50)),
    ?assertEqual(40, mfdb:update_counter(test_a, my_counter, -10)),
    %% cannot go negative
    ?assertEqual(0, mfdb:update_counter(test_a, my_counter, -100)).

ff_watcher_aa_test() ->
    ok = mfdb:subscribe(test_a, 1, {notify, info}),
    NewRec = #test_a{id = 1, value = <<"updated">>},
    ok = mfdb:insert(test_a, NewRec),
    Expect = {test_a, 1, updated, NewRec},
    receive
        Msg ->
            ?assertEqual(Expect, Msg)
    end.

ff_watcher_bb_test() ->
    ok = mfdb:subscribe(test_a, 1, {notify, info}),
    ok = mfdb:delete(test_a, 1),
    Expect = {test_a, 1, deleted},
    receive
        Msg ->
            ?assertEqual(Expect, Msg)
    end.

gg_test() ->
    Ms = [{#test_a{id = '$1', _ = '_'},[{'=<', '$1', 15}, {'>', '$1', 10}],['$_']}],
    {Recs, '$end_of_table'} = mfdb:select(test_a, Ms),
    ?assertEqual(5, length(Recs)),
    ExpectIds = [11,12,13,14,15],
    Ids = [Id || #test_a{id = Id} <- Recs],
    ?assertEqual(ExpectIds, Ids).

hh_test() ->
    [mfdb:insert(test_a, #test_a{id = X, value = integer_to_binary(X, 32)}) || X <- lists:seq(1, 100)],
    Ms = [{#test_a{id = '$1', _ = '_'},[{'=<', '$1', 100}, {'>=', '$1', 1}],['$_']}],
    %% Select returns continuations in chunks of 50 records
    {RecsA, Cont} = mfdb:select(test_a, Ms),
    ?assertEqual(true, is_function(Cont)),
    {RecsB, '$end_of_table'} = mfdb:select(Cont),
    ExpectIds = lists:seq(1,100),
    Ids = [Id || #test_a{id = Id} <- RecsA ++ RecsB],
    ?assertEqual(ExpectIds, Ids).

ii_test() ->
    ok = mfdb:clear_table(test_a),
    {ok, CWD} = file:get_cwd(),
    SourceFile = filename:join(CWD, "priv/test_import.terms"),
    Added = mfdb:import(test_a, SourceFile),
    ?assertEqual({ok, 10}, Added).

kg(L, K, D) ->
    case lists:keyfind(K, 1, L) of
        false ->
            D;
        {K, V} ->
            V
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Everything below here is
%% borrowed from couchdb-erlfdb
%%%%%%%%%%%%%%%%%%%%%%%%%%

init_test_cluster_int(Options) ->
    {ok, CWD} = file:get_cwd(),
    DefaultIpAddr = {127, 0, 0, 1},
    DefaultPort = get_available_port(),
    DefaultDir = filename:join(CWD, ".erlfdb"),

    IpAddr = kg(Options, ip_addr, DefaultIpAddr),
    Port = kg(Options, port, DefaultPort),
    Dir = kg(Options, dir, DefaultDir),
    ClusterName = kg(Options, cluster_name, <<"erlfdbtest">>),
    ClusterId = kg(Options, cluster_id, <<"erlfdbtest">>),

    DefaultClusterFile = filename:join(Dir, <<"erlfdb.cluster">>),
    ClusterFile = kg(Options, cluster_file, DefaultClusterFile),

    write_cluster_file(ClusterFile, ClusterName, ClusterId, IpAddr, Port),

    FDBServerBin = find_fdbserver_bin(Options),
    FdbServerFun = fdb_server_fun(FDBServerBin, IpAddr, Port, ClusterFile, Dir, Options),
    {FDBPid, _} = spawn_monitor(FdbServerFun),

    FDBPid ! {wait_for_init, self()},
    receive
        {initialized, FDBPid} ->
            ok;
        Msg ->
            erlang:error({fdbserver_error, Msg})
    end,

    application:set_env(mfdb, app_key, ?APP_KEY),
    application:set_env(mfdb, cluster, ClusterFile),
    {ok, ClusterFile}.

fdb_server_fun(FDBServerBin, IpAddr, Port, ClusterFile, Dir, Options) ->
    fun() ->
                                                % Open the fdbserver port
            FDBPortName = {spawn_executable, FDBServerBin},
            FDBPortArgs = [
                           <<"-p">>, ip_port_to_str(IpAddr, Port),
                           <<"-C">>, ClusterFile,
                           <<"-d">>, Dir,
                           <<"-L">>, Dir
                          ],
            FDBPortOpts = [{args, FDBPortArgs}],
            FDBServer = erlang:open_port(FDBPortName, FDBPortOpts),
            {os_pid, FDBPid} = erlang:port_info(FDBServer, os_pid),

                                                % Open the monitor pid
            MonitorPath = get_monitor_path(),
            ErlPid = os:getpid(),

            MonitorPortName = {spawn_executable, MonitorPath},
            MonitorPortArgs = [{args, [ErlPid, integer_to_binary(FDBPid)]}],
            Monitor = erlang:open_port(MonitorPortName, MonitorPortArgs),

            init_fdb_db(ClusterFile, Options),

            receive
                {wait_for_init, ParentPid} ->
                    ParentPid ! {initialized, self()}
            after 5000 ->
                    true = erlang:port_close(FDBServer),
                    true = erlang:port_close(Monitor),
                    erlang:error(fdb_parent_died)
            end,

            port_loop(FDBServer, Monitor),

            true = erlang:port_close(FDBServer),
            true = erlang:port_close(Monitor)
    end.

get_available_port() ->
    {ok, Socket} = gen_tcp:listen(0, []),
    {ok, Port} = inet:port(Socket),
    ok = gen_tcp:close(Socket),
    Port.

find_fdbserver_bin(Options) ->
    Locations = case kg(Options, fdbserver_bin, undefined) of
                    undefined ->
                        [
                         <<"/usr/sbin/fdbserver">>,
                         <<"/usr/local/sbin/fdbserver">>,
                         <<"/usr/local/libexec/fdbserver">>
                        ];
                    Else ->
                        [Else]
                end,
    case lists:filter(fun filelib:is_file/1, Locations) of
        [Path | _] -> Path;
        [] -> erlang:error(fdbserver_bin_not_found)
    end.

write_cluster_file(FileName, ClusterName, ClusterId, IpAddr, Port) ->
    Args = [ClusterName, ClusterId, ip_port_to_str(IpAddr, Port)],
    Contents = io_lib:format("~s:~s@~s~n", Args),
    ok = filelib:ensure_dir(FileName),
    ok = file:write_file(FileName, iolist_to_binary(Contents)).

ip_port_to_str({I1, I2, I3, I4}, Port) ->
    Fmt = "~b.~b.~b.~b:~b",
    iolist_to_binary(io_lib:format(Fmt, [I1, I2, I3, I4, Port])).

get_monitor_path() ->
    PrivDir = case code:priv_dir(mfdb) of
                  {error, _} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    filename:join(PrivDir, "monitor.py").

port_loop(FDBServer, Monitor) ->
    receive
        close ->
            ok;
        {FDBServer, {data, "FDBD joined cluster.\n"}} ->
                                                % Silence start message
            port_loop(FDBServer, Monitor);
        {Port, {data, Msg}} when Port == FDBServer orelse Port == Monitor ->
            io:format(standard_error, "~p", [Msg]),
            port_loop(FDBServer, Monitor);
        Error ->
            erlang:exit({fdb_cluster_error, Error})
    end.

init_fdb_db(ClusterFile, Options) ->
    DefaultFDBCli = os:find_executable("fdbcli"),
    FDBCli = case kg(Options, fdbcli_bin, DefaultFDBCli) of
                 false -> erlang:error(fdbcli_not_found);
                 FDBCli0 -> FDBCli0
             end,
    Fmt = "~s -C ~s --exec 'configure new single ssd'",
    Cmd = lists:flatten(io_lib:format(Fmt, [FDBCli, ClusterFile])),
    Res0 = os:cmd(Cmd),
    Res = hd(lists:reverse(string:tokens(Res0, "\n"))),
    case Res of
        "Database created" ++ _ -> ok;
        "ERROR: Database already exists!" ++ _ -> ok;
        Msg -> erlang:error({fdb_init_error, Msg})
    end.
