-module(mfdb_test).

-include_lib("eunit/include/eunit.hrl").

-record(test_a, {id :: integer(), value :: binary(), other :: undefined | calendar:datetime()}).
-record(test,   {id :: integer(), value :: binary(), other :: undefined | calendar:datetime()}).
-record(test_idx, {id :: integer(), value :: integer()}).
-record(test_field_ttl, {id :: integer(), value :: binary(), expires :: calendar:datetime()}).
-define(TEST_A, {test_a, [{id, integer}, {value, binary}, {other, [undefined, datetime]}]}).
-define(TEST_B, {test_b, [{id, integer}, {value, binary}, {other, [undefined, datetime]}]}).
-define(TEST_IDX, {test_idx, [{id, integer}, {value, integer}]}).
% -define(TEST_TABLE_TTL, {test_table_ttl, [{id, integer}, {value, binary}, {other, [undefined, datetime]}]}).
-define(TEST_FIELD_TTL, {test_field_ttl, [{id, integer}, {value, binary}, {expires, datetime}]}).
-define(APP_KEY, <<"cb136a3d-de40-4a85-bd10-bb23f6f1ec2a">>).
-define(IS_TX, {erlfdb_transaction, _}).

%% This sux, but it's the only way I can get around random timeout issues
t_000_test_() -> {timeout, 10, fun() -> start() end}.
t_002_test_() -> {timeout, 10, fun() -> create_and_clear_table() end}.
t_003_test_() -> {timeout, 10, fun() -> list_tables() end}.
t_004_test_() -> {timeout, 10, fun() -> basic_insert() end}.
t_005_test_() -> {timeout, 10, fun() -> basic_lookup() end}.
t_006_test_() -> {timeout, 10, fun() -> verify_records() end}.
t_007_test_() -> {timeout, 10, fun() -> counter_inc_dec() end}.
t_008_test_() -> {timeout, 10, fun() -> watch_for_insert() end}.
t_009_test_() -> {timeout, 10, fun() -> watch_for_update() end}.
t_010_test_() -> {timeout, 10, fun() -> watch_for_delete() end}.
t_011_test_() -> {timeout, 10, fun() -> select_no_continuation() end}.
t_012_test_() -> {timeout, 10, fun() -> select_with_continuation() end}.
t_013_test_() -> {timeout, 10, fun() -> import_from_terms_file() end}.
t_014_test_() -> {timeout, 10, fun() -> check_field_types() end}.
t_015_test_() -> {timeout, 60, fun() -> fold_simple() end}.
t_016_test_() -> {timeout, 60, fun() -> fold_limit() end}.
t_017_test_() -> {timeout, 60, fun() -> fold_update() end}.
t_018_test_() -> {timeout, 60, fun() -> fold_delete() end}.
t_019_test_() -> {timeout, 60, fun() -> parallel_fold_delete() end}.
t_020_test_() -> {timeout, 60, fun() -> parallel_fold_delete2() end}.
t_021_test_() -> {timeout, 60, fun() -> indexed_select_one() end}.
t_022_test_() -> {timeout, 60, fun() -> indexed_select_continuation() end}.
t_023_test_() -> {timeout, 60, fun() -> indexed_fold() end}.
t_024_test_() -> {timeout, 120, fun() -> field_ttl() end}.
t_099_test_() -> {timeout, 10, fun() -> stop() end}.

start() ->
    {ok, _ClusterFile} = init_test_cluster_int([]),
    {Ok, _} = application:ensure_all_started(mfdb),
    ?assertEqual(ok, Ok).

stop() ->
    %% Delete everything from the test database
    Db = mfdb_conn:connection(),
    erlfdb:clear_range(Db, <<>>, <<16#FF>>),
    true.

reset_table(test_a, Count) ->
    ok = mfdb:clear_table(test_a),
    [mfdb:insert(test_a, #test_a{id = X, value = integer_to_binary(X, 32)}) || X <- lists:seq(1, Count)],
    timer:sleep(500),
    ok;
reset_table(test_idx, Count) ->
    ok = mfdb:clear_table(test_idx),
    [mfdb:insert(test_idx, #test_idx{id = X, value = integer_to_binary(X, 32)}) || X <- lists:seq(1, Count)],
    timer:sleep(500),
    ok.

create_and_clear_table() ->
    Ok = mfdb:create_table(test_a, [{record, ?TEST_A}]),
    Ok = mfdb:clear_table(test_a),
    ?assertEqual(ok, Ok).

% create_and_clear_table_ttl() ->
%     Ok = mfdb:create_table(test_table_ttl, [{record, ?TEST_TABLE_TTL}, {table_ttl, {minutes, 1}}]),
%     Ok = mfdb:clear_table(test_table_ttl),
%     ?assertEqual(ok, Ok).

create_and_clear_field_ttl() ->
    Ok = mfdb:create_table(test_field_ttl, [{record, ?TEST_FIELD_TTL}, {field_ttl, 4}]),
    Ok = mfdb:clear_table(test_field_ttl),
    ?assertEqual(ok, Ok).

list_tables() ->
    ok = mfdb:create_table(test_b, [{record, ?TEST_B}]),
    {ok, Tabs} = mfdb:table_list(),
    ?assertEqual([test_a, test_b], Tabs),
    ok = mfdb:delete_table(test_b),
    ?assertEqual({error,not_connected}, mfdb:table_info(test_b, all)),
    ?assertEqual({error,no_such_table}, mfdb:connect(test_b)).

basic_insert() ->
    ok = mfdb:clear_table(test_a),
    [mfdb:insert(test_a, #test_a{id = X, value = integer_to_binary(X, 32)}) || X <- lists:seq(1, 50)],
    {ok, Count} = mfdb:table_info(test_a, count),
    ?assertEqual(50, Count).

basic_lookup() ->
    reset_table(test_a, 50),
    Rec50In = #test_a{id = 50, value = integer_to_binary(50, 32)},
    {ok, Rec50Out} = mfdb:lookup(test_a, 50),
    ?assertEqual(Rec50In, Rec50Out).

verify_records() ->
    reset_table(test_a, 50),
    IdSumIn = lists:sum([X || X <- lists:seq(1, 50)]),
    IdSumOut = mfdb:fold(test_a, fun(#test_a{id = X}, Acc) -> Acc + X end, 0),
    ?assertEqual(IdSumIn, IdSumOut).

counter_inc_dec() ->
    ?assertEqual(50, mfdb:update_counter(test_a, my_counter, 50)),
    ?assertEqual(40, mfdb:update_counter(test_a, my_counter, -10)),
    mfdb:set_counter(test_a, my_counter, 11),
    ?assertEqual(10, mfdb:update_counter(test_a, my_counter, -1)),
    %% cannot go negative
    ?assertEqual(-90, mfdb:update_counter(test_a, my_counter, -100)).

watch_for_insert() ->
    mfdb:clear_table(test_a),
    Id = 51,
    ok = mfdb:subscribe(test_a, Id, {notify, info}),
    DateTime = erlang:universaltime(),
    NewRec = #test_a{id = Id, value = <<"updated">>, other = DateTime},
    ok = mfdb:insert(test_a, NewRec),
    Expect = {test_a, Id, created, NewRec},
    receive
        Msg ->
            ?assertEqual(Expect, Msg),
            ok = mfdb:unsubscribe(test_a, Id)
    end.

watch_for_update() ->
    ok = mfdb:clear_table(test_a),
    Id = 1,
    ok = mfdb:insert(test_a, #test_a{id = Id, value = <<"BaseVal">>, other = undefined}),
    timer:sleep(100),
    ok = mfdb:subscribe(test_a, Id, {notify, info}),
    DateTime = erlang:universaltime(),
    ExpectRec = #test_a{id = Id, value = <<"updated">>, other = DateTime},
    Changes = [{value, <<"updated">>}, {other, DateTime}],
    ok = mfdb:update(test_a, Id, Changes),
    Expect = {test_a, Id, updated, ExpectRec},
    receive
        Msg ->
            ?assertEqual(Expect, Msg),
            ok = mfdb:unsubscribe(test_a, Id)
    end.

watch_for_delete() ->
    mfdb:clear_table(test_a),
    Id = 1,
    ok = mfdb:insert(test_a, #test_a{id = Id, value = <<"BaseVal">>, other = undefined}),
    timer:sleep(100),
    ok = mfdb:subscribe(test_a, Id, {notify, info}),
    ok = mfdb:delete(test_a, Id),
    Expect = {test_a, Id, deleted},
    receive
        Msg ->
            ?assertEqual(Expect, Msg),
            ok = mfdb:unsubscribe(test_a, Id)
    end.

select_no_continuation() ->
    reset_table(test_a, 20),
    Ms = [{#test_a{id = '$1', _ = '_'},[{'=<', '$1', 15}, {'>', '$1', 10}],['$_']}],
    {Recs, '$end_of_table'} = mfdb:select(test_a, Ms),
    ?assertEqual(5, length(Recs)),
    ExpectIds = [11,12,13,14,15],
    Ids = [Id || #test_a{id = Id} <- Recs],
    ?assertEqual(ExpectIds, Ids).

select_with_continuation() ->
    reset_table(test_a, 100),
    Ms = [{#test_a{id = '$1', _ = '_'},[{'=<', '$1', 100}, {'>=', '$1', 1}],['$_']}],
    %% Select returns continuations in chunks of 50 records
    {RecsA, Cont} = mfdb:select(test_a, Ms),
    ?assertEqual(true, is_function(Cont)),
    {RecsB, '$end_of_table'} = mfdb:select(Cont),
    ExpectIds = lists:seq(1,100),
    Ids = [Id || #test_a{id = Id} <- RecsA ++ RecsB],
    ?assertEqual(ExpectIds, Ids).

import_from_terms_file() ->
    ok = mfdb:clear_table(test_a),
    {ok, CWD} = file:get_cwd(),
    SourceFile = filename:join(CWD, "priv/test_import.terms"),
    Added = mfdb:import(test_a, SourceFile),
    ?assertEqual({ok, 10}, Added).

check_field_types() ->
    ok = mfdb:clear_table(test_a),
    Error0 = mfdb:insert(test_a, #test_a{id = <<"one">>}),
    ?assertEqual({error,{id,<<"one">>,not_integer}}, Error0),
    Error1 = mfdb:insert(test_a, #test_a{id = 1, value = name_atom}),
    ?assertEqual({error,{value,name_atom,not_binary}}, Error1),
    Error2 = mfdb:insert(test_a, #test{id = 1, value = <<"value">>}),
    ?assertEqual({error,invalid_record}, Error2).

fold_simple() ->
    reset_table(test_a, 500),
    timer:sleep(500),
    FoldFun = fun(#test_a{id = Id}, {Cnt, Sum}) -> {Cnt + 1, Sum + Id} end,
    Expected = {500, lists:sum(lists:seq(1,500))},
    Res = mfdb:fold(test_a, FoldFun, {0, 0}),
    ?assertEqual(Expected, Res).

fold_limit() ->
    reset_table(test_a, 500),
    timer:sleep(500),
    FoldFun = fun(#test_a{id = Id}, {Cnt, Sum}) when Cnt < 250 ->
                      {Cnt + 1, Sum + Id};
                 (_, Acc) ->
                      {'$exit_fold', Acc}
              end,
    Expected = {250, lists:sum(lists:seq(1,250))},
    Res = mfdb:fold(test_a, FoldFun, {0, 0}),
    ?debugFmt("fold_limit got: ~p", [Res]),
    ?assertEqual(Expected, Res).

fold_update() ->
    reset_table(test_a, 10),
    {ok, TabCnt} = mfdb:table_info(test_a, count),
    ?assertEqual(10, TabCnt),
    FoldFun = fun(Tx, #test_a{id = Id, value = _Val}, Cnt) ->
                      NewVal = integer_to_binary(Id * 2, 32),
                      ok = mfdb:update(Tx, Id, [{value, NewVal}]),
                      Cnt + 1
              end,
    Res = mfdb:fold(test_a, FoldFun, 0),
    ?assertEqual(10, Res),
    Expected = lists:sum([X * 2 || X <- lists:seq(1,10)]),
    SumFun = fun(#test_a{value = Val}, S) ->
                     S + binary_to_integer(Val, 32)
             end,
    Sum = mfdb:fold(test_a, SumFun, 0),
    ?assertEqual(Expected, Sum).

parallel_fold_delete() ->
    %% Multiple processes folding over and deleting data from a table
    %% Ensure only 500 records (count for table) are actually processed
    reset_table(test_a, 500),
    {ok, TabCnt} = mfdb:table_info(test_a, count),
    ?assertEqual(500, TabCnt),
    FoldDeleteFun =
        fun(Tx, #test_a{id = Id}, Acc) ->
                try ok = mfdb:delete(Tx, Id),
                     Acc + 1
                catch
                    _E:_M:_Stack ->
                        %% error_logger:error_msg("Skipped ~p because ~p", [Id, {E,M,Stack}]),
                        Acc
                end
        end,
    Self = self(),
    Start = erlang:monotonic_time(millisecond),
    Fold = fun(X) -> Self ! {X, mfdb:fold(test_a, FoldDeleteFun, 0)} end,
    Ids = lists:seq(1,5),
    [spawn(fun() -> Fold(X) end) || X <- Ids],
    Results = recv(Ids, []),
    End = erlang:monotonic_time(millisecond),
    Time = End - Start,
    ?debugFmt("5-process parallel fold of 500 with delete took ~p sec", [Time/1000]),
    ?assertEqual(500, lists:sum([C || {_, C} <- Results])).

parallel_fold_delete2() ->
    %% Multiple processes folding over and deleting data from a table
    %% Ensure only 500 records (count for table) are actually processed
    reset_table(test_a, 500),
    {ok, TabCnt} = mfdb:table_info(test_a, count),
    ?assertEqual(500, TabCnt),
    FoldDeleteFun =
        fun(#test_a{id = Id}, Acc) ->
                [Id | Acc]
        end,
    Self = self(),
    Start = erlang:monotonic_time(millisecond),
    RecIds =  mfdb:fold(test_a, FoldDeleteFun, []),

    DeleteFun = fun(X) -> Self ! {X, lists:foldl(fun(Id, Acc) ->
                                                         try mfdb:delete_if_exists(test_a, Id) of
                                                            ok ->
                                                              Acc + 1;
                                                            {error, not_found} ->
                                                              Acc
                                                         catch
                                                             _E:_M:_Stack ->
                                                                ?debugFmt("Skipped ~p because ~p", [Id, {_E,_M,_Stack}]),
                                                                 Acc
                                                         end
                                                 end, 0, RecIds)} end,
    Ids = lists:seq(1,5),
    [spawn(fun() -> DeleteFun(X) end) || X <- Ids],
    Results = recv(Ids, []),
    End = erlang:monotonic_time(millisecond),
    Time = End - Start,
    ?debugFmt("5-process parallel fold of 500 with delayed delete took ~p sec", [Time/1000]),
    ?assertEqual(500, lists:sum([C || {_, C} <- Results])).

fold_delete() ->
    %% Multiple processes folding over and deleting data from a table
    %% Ensure only 500 records (count for table) are actually processed
    reset_table(test_a, 500),
    {ok, TabCnt} = mfdb:table_info(test_a, count),
    ?assertEqual(500, TabCnt),
    FoldDeleteFun =
        fun(#test_a{id = Id}, Acc) ->
                try ok = mfdb:delete(test_a, Id),
                     Acc + 1
                catch
                    _E:_M:_Stack ->
                        %% error_logger:error_msg("Skipped ~p because ~p", [Id, {E,M,Stack}]),
                        Acc
                end
        end,
    Start = erlang:monotonic_time(millisecond),
    Cnt = mfdb:fold(test_a, FoldDeleteFun, 0),
    End = erlang:monotonic_time(millisecond),
    Time = End - Start,
    ?debugFmt("fold of 500 with delete took ~p sec", [Time/1000]),
    ?assertEqual(500, Cnt).

indexed_select_one() ->
    _ = mfdb:delete_table(test_idx),
    ok = mfdb:create_table(test_idx, [{record, ?TEST_IDX}, {indexes, [3]}]),
    [spawn(fun() -> ok = mfdb:insert(test_idx, #test_idx{id = X, value = X}) end) || X <- lists:seq(1,100)],
    timer:sleep(500),
    {D, '$end_of_table'} = mfdb:select(test_idx, [{#test_idx{value = '$1', _ = '_'}, [{'>', '$1', 90}, {'<', '$1', 96}], ['$_']}]),
    NumRes = length(D),
    ?assertEqual(5, NumRes).

indexed_select_continuation() ->
    _ = mfdb:delete_table(test_idx),
    ok = mfdb:create_table(test_idx, [{record, ?TEST_IDX}, {indexes, [3]}]),
    [spawn(fun() -> ok = mfdb:insert(test_idx, #test_idx{id = X, value = X}) end) || X <- lists:seq(1,100)],
    timer:sleep(500),
    WrapFun = fun WrapFun({D, '$end_of_table'}, InAcc) ->
                      lists:append(InAcc, D);
                  WrapFun({D, Cont}, InAcc) ->
                      WrapFun(mfdb:select(Cont), lists:append(InAcc, D))
              end,
    Res = WrapFun(mfdb:select(test_idx, [{#test_idx{value = '$1', _ = '_'}, [{'>', '$1', 0}], ['$_']}]), []),
    NumRes = length(Res),
    ?assertEqual(100, NumRes).

indexed_fold() ->
    _ = mfdb:delete_table(test_idx),
    ok = mfdb:create_table(test_idx, [{record, ?TEST_IDX}, {indexes, [3]}]),
    [spawn(fun() -> ok = mfdb:insert(test_idx, #test_idx{id = X, value = X}) end) || X <- lists:seq(1,100)],
    timer:sleep(500),
    FoldFun = fun(#test_idx{id = Id}, Acc) -> [Id | Acc] end,
    D = mfdb:fold(test_idx, FoldFun, [], [{#test_idx{value = '$1', _ = '_'}, [{'>', '$1', 90}, {'<', '$1', 96}], ['$_']}]),
    ?assertEqual([95,94,93,92,91], D).

field_ttl() ->
    ok = create_and_clear_field_ttl(),
    %% We want an expiration in the past
    Expires = mfdb_lib:expires({minutes, -5}),
    NotExpires = mfdb_lib:expires({minutes, 5}),
    %% ?debugFmt("Now: ~p Expires: ~p NotExpires: ~p", [erlang:universaltime(), Expires, NotExpires]),
    [mfdb:insert(test_field_ttl, #test_field_ttl{id = X, value = integer_to_binary(X, 32), expires = NotExpires}) || X <- lists:seq(1, 10)],
    [mfdb:insert(test_field_ttl, #test_field_ttl{id = X, value = integer_to_binary(X, 32), expires = Expires}) || X <- lists:seq(11, 20)],
    [mfdb:insert(test_field_ttl, #test_field_ttl{id = X, value = integer_to_binary(X, 32), expires = NotExpires}) || X <- lists:seq(21, 30)],
    {ok, Count} = mfdb:table_info(test_field_ttl, count),
    ?assertEqual(30, Count),
    {ok, Pid} = mfdb_reaper:do_reap(test_field_ttl),
    erlang:monitor(process, Pid),
    receive
        {'DOWN', _, _, _, _} ->
            ok
    after 5000 ->
            throw(timeout)
    end,
    {ok, Count2} = mfdb:table_info(test_field_ttl, count),
    ?assertEqual(20, Count2),
    ?assertEqual(20, mfdb:fold(test_field_ttl, fun(_,C) -> C + 1 end, 0)).

recv([], Acc) ->
    Acc;
recv(Ids, Acc) ->
    receive
        {X, _Cnt} = Msg ->
            recv(lists:delete(X, Ids), [Msg | Acc])
    after 60000 ->
            throw(timeout)
    end.

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
