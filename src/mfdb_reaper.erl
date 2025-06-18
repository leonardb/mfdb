%%% @copyright 2020 Leonard Boyce <leonard.boyce@lucidlayer.com>
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
%%%
%%% A bunch of this code was derived from mnesia_rocksdb
%%% https://github.com/aeternity/mnesia_rocksdb
%%% and where applicable copyright remains with them

-module(mfdb_reaper).

-behaviour(gen_server).

-export([
    debug/1,
    do_reap/1,
    do_reap/2,
    cleanup_orphaned/1
]).

%% gen_server API
-export([
    start_link/1,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("mfdb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-else.
-define(debugFmt(F, A), ok).
-endif.

-include_lib("kernel/include/logger.hrl").
-define(REAP_POLL_INTERVAL, 5000).
-define(REAP_SEGMENT_SIZE, 200).
-define(REAP_CALLBACK_PER_PROCESS, 10).
-define(ndbg(Dbg, Fmt, Args),
    case Dbg of
        true -> ?LOG_INFO(Fmt, Args);
        _ -> ok
    end
).

debug(Table) ->
    gen_server:call(?REAPERPROC(Table), debug).

do_reap(Table) ->
    do_reap(Table, #{
        debug => false, segment_size => ?REAP_SEGMENT_SIZE, expire_tstamp => erlang:universaltime()
    }).

do_reap(Table, Opts) when is_map(Opts) ->
    gen_server:call(?REAPERPROC(Table), {do_reap, Table, Opts});
do_reap(_Tab, _Opts) ->
    {error,
        <<"Call as do_reap(Table :: atom(), Opts :: #{debug := boolean(), segment_size := pos_integer()}).">>}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%% @private
start_link(Table) ->
    gen_server:start_link(?REAPERPROC(Table), ?MODULE, [Table], []).

%% @private
init([Table]) ->
    %% Only start a reaper poller if table has TTL
    #st{ttl = Ttl} = mfdb_manager:st(Table),
    Poller =
        case Ttl of
            undefined ->
                undefined;
            _ ->
                poll_timer(undefined)
        end,
    {ok, #{table => Table, poller => Poller}}.

handle_call(stop, _From, State) ->
    case maps:get(reaper, State, undefined) of
        undefined ->
            {stop, normal, ok, State};
        {Pid, Ref} ->
            demonitor(Ref),
            exit(Pid, kill),
            {stop, normal, ok, State}
    end;
handle_call({do_reap, Table, Opts}, _From, #{table := Table} = State) ->
    Debug = maps:get(debug, Opts, false),
    ExpireTstamp = maps:get(expire_tstamp, Opts, erlang:universaltime()),
    SegmentSize = maps:get(segment_size, Opts, ?REAP_SEGMENT_SIZE),
    Pid = spawn(fun() -> reap_expired_loop_(Table, SegmentSize, Debug, ExpireTstamp, 0) end),
    {reply, {ok, Pid}, State};
handle_call(debug, _From, State) ->
    Debug = maps:get(debug, State, false) =:= false,
    {reply, Debug, State#{debug => Debug}};
handle_call(_, _, State) ->
    {reply, error, State}.

%% @private
handle_cast(_, State) ->
    {noreply, State}.

%% @private
handle_info(timeout, #{table := Table} = State) ->
    %% Non-blocking reaping of expired records from table
    Debug = maps:get(debug, State, false),
    ExpireTstamp = erlang:universaltime(),
    try inside_reap_window() andalso reap_expired_(Table, ?REAP_SEGMENT_SIZE, ExpireTstamp) of
        false ->
            ?ndbg(Debug, "Not in reaping window", []),
            {noreply, State};
        0 ->
            ?ndbg(Debug, "Table ~p reaping complete", [Table]),
            {noreply, State};
        Cnt ->
            ?ndbg(Debug, "Table ~p reaped ~p", [Table, Cnt]),
            {noreply, State, 0}
    catch
        E:M:St ->
            error_logger:error_msg("poller crash: ~p ~p ~p", [E, M, St]),
            {noreply, State}
    end;
handle_info(poll, #{table := Table, poller := Poller} = State) ->
    %% Non-blocking reaping of expired records from table
    Debug = maps:get(debug, State, false),
    ExpireTstamp = erlang:universaltime(),
    try inside_reap_window() andalso reap_expired_(Table, ?REAP_SEGMENT_SIZE, ExpireTstamp) of
        false ->
            ?ndbg(Debug, "Not in reaping window", []),
            {noreply, State#{poller => poll_timer(Poller)}};
        0 ->
            ?ndbg(Debug, "Table ~p reaping complete", [Table]),
            {noreply, State#{poller => poll_timer(Poller)}};
        Cnt ->
            ?ndbg(Debug, "Table ~p reaped ~p", [Table, Cnt]),
            self() ! poll,
            {noreply, State}
    catch
        E:M:St ->
            error_logger:error_msg("poller crash: ~p ~p ~p", [E, M, St]),
            {noreply, State#{poller => poll_timer(Poller)}}
    end;
handle_info(_UNKNOWN, State) ->
    {noreply, State}.

%% @private
terminate(_, _) ->
    ok.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%% @private
poll_timer(undefined) ->
    poll_timer(undefined, ?REAP_POLL_INTERVAL);
poll_timer(TRef) when is_reference(TRef) ->
    poll_timer(TRef, ?REAP_POLL_INTERVAL).

%% @private
poll_timer(undefined, T) ->
    erlang:send_after(T, self(), poll);
poll_timer(TRef, T) when is_reference(TRef) ->
    erlang:cancel_timer(TRef),
    erlang:send_after(T, self(), poll).

%% @private
reap_expired_loop_(Table, SegmentSize, Debug, ExpireTstamp, Total) ->
    case reap_expired_(Table, SegmentSize, ExpireTstamp) of
        0 ->
            ?debugFmt("Reaping completed table ~p : ~w", [Table, Total]),
            ?ndbg(Debug, "Reaping completed table ~p : ~w", [Table, Total]),
            ok;
        ok ->
            ?debugFmt("Reaping completed table ~p : ~w", [Table, Total]),
            ?ndbg(Debug, "Reaping completed table ~p : ~w", [Table, Total]),
            ok;
        Count ->
            ?debugFmt("Reaped ~w from table ~p : ~w", [Count, Table, Total + Count]),
            ?ndbg(Debug, "Reaped ~w from table ~p : ~w", [Count, Table, Total + Count]),
            reap_expired_loop_(Table, SegmentSize, Debug, ExpireTstamp, Total + Count)
    end.

%% @private
reap_expired_(Table, SegmentSize, ExpireTstamp) ->
    #st{pfx = TabPfx, ttl = Ttl} = St = mfdb_manager:st(Table),
    case Ttl of
        undefined ->
            0;
        _ ->
            RangeStart = mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, ?FDB_WC, ?FDB_WC}),
            RangeEnd = erlfdb_key:strinc(
                mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, ExpireTstamp, ?FDB_END})
            ),
            reap_expired_(St, RangeStart, RangeEnd, ExpireTstamp, SegmentSize)
    end.

%% @private
reap_expired_(#st{db = Db, pfx = TabPfx0} = St, RangeStart, RangeEnd, ExpireTstamp, SegmentSize) ->
    KVs = mfdb_lib:wait(erlfdb:get_range(Db, RangeStart, RangeEnd, [{limit, SegmentSize}])),
    LastKey = lists:foldl(
        fun({EncKey, <<>>}, LastKey) ->
            %% Delete the actual expired record
            <<PfxBytes:8, TabPfx/binary>> = TabPfx0,
            <<PfxBytes:8, TabPfx:PfxBytes/binary, EncValue/binary>> = EncKey,
            case sext:decode(EncValue) of
                {?TTL_TO_KEY_PFX, Expires, RecKey} when Expires < ExpireTstamp ->
                    try
                        ok = mfdb_lib:delete(St, RecKey),
                        %% Key2Ttl have to be removed individually (now done in mfdb_lib:delete/2)
                        %%TtlK2T = mfdb_lib:encode_key(TabPfx, {?KEY_TO_TTL_PFX, RecKey}),
                        %%ok = mfdb_lib:wait(erlfdb:clear(Tx, TtlK2T)),
                        mfdb_lib:wait(erlfdb:clear(Db, EncKey)),
                        EncKey
                    catch
                        _E:_M:_Stack ->
                            LastKey
                    end;
                _Val ->
                    LastKey
            end
        end,
        ok,
        KVs
    ),
    case LastKey of
        ok ->
            0;
        LastKey ->
            Count = length(KVs),
            %% mfdb_lib:wait(erlfdb:clear_range(Tx, RangeStart, erlfdb_key:strinc(LastKey))),
            Count
    end.

%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec inside_reap_window() -> boolean().
inside_reap_window() ->
    case
        {
            application:get_env(mfdb, reap_window_min, undefined),
            application:get_env(mfdb, reap_window_max, undefined)
        }
    of
        {undefined, undefined} ->
            true;
        {undefined, _} ->
            true;
        {_, undefined} ->
            true;
        {Min, Max} ->
            {_Date, Time} = erlang:universaltime(),
            case Time > Min andalso Time < Max of
                true ->
                    true;
                false ->
                    false
            end
    end.

cleanup_orphaned(T) ->
    spawn_link(fun() ->
        mfdb:connect(T),
        R = persistent_term:get({mfdb, smlib:to_bin(T)}),
        Final = lists:foldl(
            fun(I, Acc) ->
                Start =
                    case I of
                        2 ->
                            mfdb_lib:encode_prefix(R#st.pfx, {<<"ttl-k2t">>, '_'});
                        3 ->
                            mfdb_lib:encode_prefix(R#st.pfx, {'_', '_', '_'})
                    end,
                End =
                    case I of
                        2 ->
                            erlfdb_key:strinc(
                                mfdb_lib:encode_prefix(R#st.pfx, {<<"ttl-k2t">>, <<"~">>})
                            );
                        3 ->
                            erlfdb_key:strinc(
                                mfdb_lib:encode_prefix(R#st.pfx, {<<"~">>, <<"~">>, <<"~">>})
                            )
                    end,
                Res = orphaned_fold_(T, R, Start, End, #{total => 0, del_count => 0, del_ids => []}),
                io:format("END: ~p ~p~n", [I, Res]),
                Pids = [spawn_link(mfdb, delete, T, K) || K <- maps:get(del_ids, Res, [])],
                wait_for(Pids),
                Acc#{{T, I} => maps:without([del_ids, del_count], Res)}
            end,
            #{},
            [2, 3]
        ),
        io:format("FINAL: ~p~n", [Final])
    end).

wait_for([]) ->
    ok;
wait_for(Pids) ->
    receive
        {'DOWN', _Ref, process, Pid, _Reason} ->
            wait_for(lists:delete(Pid, Pids))
    after 5000 ->
        ok
    end.

orphaned_fold_(T, R, LastKey, End, InAcc) ->
    {NAcc, NLastKey, Cnt} =
        erlfdb:fold_range(
            R#st.db,
            {LastKey, gt},
            End,
            fun({K, V}, {#{total := _Tot, del_ids := DelIds, del_count := DelCount} = Acc0, _, C}) ->
                Acc1 =
                    case DelCount of
                        100 ->
                            io:format("Deleting ~p orphaned records from table ~p~n", [
                                DelCount, T
                            ]),
                            Pids = [spawn_link(mfdb, delete, T, Dk) || Dk <- DelIds],
                            wait_for(Pids),
                            Acc0#{
                                del_ids => [],
                                del_count => 0
                            };
                        _ ->
                            Acc0
                    end,
                {Kt, KtVal} = key_type(R#st.pfx, K),
                Acc2 =
                    case Kt of
                        key_to_ttl_pfx ->
                            case KtVal of
                                {<<"ttl-k2t">>, Pk} ->
                                    case
                                        erlfdb:get(
                                            R#st.db, mfdb_lib:encode_key(R#st.pfx, {<<"dd">>, Pk})
                                        )
                                    of
                                        not_found ->
                                            Acc1#{
                                                del_count => maps:get(del_count, Acc1, 0) + 1,
                                                del_ids => [Pk | maps:get(del_ids, Acc1, [])]
                                            };
                                        _ ->
                                            ok
                                    end;
                                _ ->
                                    ok
                            end;
                        ttl_to_key_pfx ->
                            case KtVal of
                                {<<"ttl-t2k">>, _, Pk} ->
                                    case
                                        erlfdb:get(
                                            R#st.db, mfdb_lib:encode_key(R#st.pfx, {<<"dd">>, Pk})
                                        )
                                    of
                                        not_found ->
                                            Acc1#{
                                                del_count => maps:get(del_count, Acc1, 0) + 1,
                                                del_ids => [Pk | maps:get(del_ids, Acc1, [])]
                                            };
                                        _ ->
                                            ok
                                    end;
                                _ ->
                                    ok
                            end;
                        _ ->
                            ok
                    end,
                Nc = C + 1,
                NAcc =
                    case Acc2 of
                        #{Kt := #{c := Old, s := OldSize}} ->
                            Acc2#{Kt => #{c => Old + 1, s => OldSize + byte_size(K) + get_size(V)}};
                        _ ->
                            Acc2#{Kt => #{c => 1, s => byte_size(K) + get_size(V)}}
                    end,
                %% io:format("~s ~w ~p~n", [Tab, Nc, NAcc]),
                %% io:format("~s ~w ~p~n", [Tab, Nc, mfdb_lib:decode_key(R#st.pfx, K)]),
                %case ((Tot + Nc) rem 5000) =:= 0 andalso Nc > 0 of
                %    true ->
                %        io:format("INSIDE: ~s ~w ~p~n", [Tab, Nc, mfdb_lib:decode_key(R#st.pfx, K)]);
                %    false ->
                %        ok
                %end,
                {NAcc, K, Nc}
            end,
            {InAcc, undefined, 0},
            [{limit, 500}]
        ),
    case Cnt of
        0 ->
            NAcc;
        _ ->
            NTotal = maps:get(total, NAcc, 0) + Cnt,
            io:format("~s ~w ~p ~p~n", [
                T, NTotal, NAcc, catch mfdb_lib:decode_key(R#st.pfx, NLastKey)
            ]),
            orphaned_fold_(T, R, NLastKey, End, NAcc#{total => NTotal})
    end.

key_type(
    <<PfxBytes:8, TabPfx/binary>>,
    <<PfxBytes:8, TabPfx:PfxBytes/binary, EncValue/binary>>
) ->
    case sext:decode(EncValue) of
        {<<"id", _/binary>>, _} = D ->
            %% data index reference key
            %% so wrap in a tuple
            {idx_data_prefix, D};
        {<<"dd">>, _Key} = D ->
            {data_pfx, D};
        {<<"cc">>, _Key} = D ->
            {counter_pfx, D};
        {<<"cc">>, _Key, _} = D ->
            {counter_pfx, D};
        {<<"tc">>, _Key} = D ->
            {table_cnt_pfx, D};
        {<<"ts">>, _Key} = D ->
            {table_size_pfx, D};
        {<<"ic">>, _Key} = D ->
            {idx_count_pfx, D};
        {<<"mfdb_ref">>, _PartHcaVal} = D ->
            {mfdb_ref_pfx, D};
        {<<"pt">>, _PartHcaVal} = D ->
            {data_part_pfx, D};
        {<<"ttl-t2k">>, _, _Key} = D ->
            {ttl_to_key_pfx, D};
        {<<"ttl-k2t">>, _Key} = D ->
            {key_to_ttl_pfx, D};
        BadVal ->
            io:format("badval: ~p~n", [BadVal]),
            {unknown, undefined}
    end.

get_size(X) when is_binary(X) -> byte_size(X);
get_size(X) -> byte_size(term_to_binary(X)).
