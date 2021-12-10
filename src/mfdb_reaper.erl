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

%% gen_server API
-export([start_link/1,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([test_callback/1]).

-include("mfdb.hrl").
-define(REAP_POLL_INTERVAL, 500).
-define(REAP_SEGMENT_SIZE, 200).
-define(REAP_CALLBACK_PER_PROCESS, 10).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%% @private
start_link(Table) ->
    gen_server:start_link(?REAPERPROC(Table), ?MODULE, [Table], []).

%% @private
init([Table]) ->
    %% Only start a reaper poller if table has TTL
    #st{ttl = Ttl} = mfdb_manager:st(Table),
    Poller = case Ttl of
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
handle_call(_, _, State) ->
    {reply, error, State}.

%% @private
handle_cast(_, State) ->
    {noreply, State}.

%% @private
handle_info(poll, #{table := Table, poller := Poller} = State) ->
    %% Non-blocking reaping of expired records from table
    case maps:get(reaper, State, undefined) of
        undefined ->
            PidRef = init_reaper(Table),
            {noreply, State#{poller => poll_timer(Poller), reaper => PidRef}};
        _ ->
            {noreply, State#{poller => poll_timer(Poller)}}
    end;
handle_info({'DOWN', Ref, process, _Pid0, Reason}, #{table := Table, poller := Poller} = State) ->
    case maps:get(reaper, State, undefined) of
        undefined ->
            {noreply, State#{poller => poll_timer(Poller)}};
        {_Pid1, Ref} ->
            erlang:demonitor(Ref),
            case Reason of
                {normal, X} when X > 0 ->
                    %% We want a 'short' poll to allow other nodes to grab locks
                    %% on segments of expired data to improve node concurrency
                    {noreply, State#{poller => poll_timer(Poller, 100), reaper => undefined}};
                _ ->
                    %% io:format("No expired records in ~p~n", [Table]),
                    {noreply, State#{poller => poll_timer(Poller), reaper => undefined}}
            end;
        _PidRef ->
            %% Unmatched ref???
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
init_reaper(Table) ->
    spawn_monitor(fun() -> Cnt = reap_expired_(Table), exit({normal, Cnt}) end).

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
reap_expired_(Table) ->
    #st{pfx = TabPfx, ttl = Ttl, ttl_callback = TtlCb} = St = mfdb_manager:st(Table),
    case Ttl of
        undefined ->
            %% No expiration
            0;
        _ ->
            Now = erlang:universaltime(),
            RangeStart = mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, ?FDB_WC, ?FDB_WC}),
            RangeEnd = erlfdb_key:strinc(mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, Now, ?FDB_END})),
            TtlModFun = case Ttl of
                            {field, _FieldIdx} ->
                                case TtlCb of
                                    {_,_} ->
                                        TtlCb;
                                    _ ->
                                        undefined
                                end;
                            _ ->
                                undefined
                        end,
            reap_expired_(St, TtlModFun, RangeStart, RangeEnd, Now)
    end.

%% @private
reap_expired_(#st{db = Db, pfx = TabPfx0} = St, undefined, RangeStart, RangeEnd, Now) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              KVs = mfdb_lib:wait(erlfdb:get_range(Tx, RangeStart, RangeEnd, [{limit, ?REAP_SEGMENT_SIZE}])),
              LastKey = lists:foldl(
                          fun({EncKey, <<>>}, LastKey) ->
                                  %% Delete the actual expired record
                                  <<PfxBytes:8, TabPfx/binary>> = TabPfx0,
                                  <<PfxBytes:8, TabPfx:PfxBytes/binary, EncValue/binary>> = EncKey,
                                  case sext:decode(EncValue) of
                                      {?TTL_TO_KEY_PFX, Expires, RecKey} when Expires < Now ->
                                          try
                                              ok = mfdb_lib:delete(St#st{db = Tx}, RecKey),
                                              %% Key2Ttl have to be removed individually
                                              TtlK2T = mfdb_lib:encode_key(TabPfx, {?KEY_TO_TTL_PFX, RecKey}),
                                              ok = mfdb_lib:wait(erlfdb:clear(Tx, TtlK2T)),
                                              EncKey
                                          catch
                                              _E:_M:_Stack ->
                                                  LastKey
                                          end;
                                      _ ->
                                          LastKey
                                  end
                          end, ok, KVs),
              case LastKey of
                  ok ->
                      ok;
                  LastKey ->
                      mfdb_lib:wait(erlfdb:clear_range(Tx, RangeStart, erlfdb_key:strinc(LastKey)))
              end,
              length(KVs)
      end);
reap_expired_(#st{db = Db, pfx = TabPfx0} = St, TtlModFun, RangeStart, RangeEnd, Now) ->
    {CbRecs, _Cnt} =
        erlfdb:transactional(
          Db,
          fun(Tx) ->
                  KVs = mfdb_lib:wait(erlfdb:get_range(Tx, RangeStart, RangeEnd, [{limit, ?REAP_SEGMENT_SIZE}])),
                  ICbRecs = lists:foldl(
                              fun({EncKey, <<>>}, IAcc) ->
                                      %% Delete the actual expired record
                                      <<PfxBytes:8, TabPfx/binary>> = TabPfx0,
                                      <<PfxBytes:8, TabPfx:PfxBytes/binary, EncValue/binary>> = EncKey,
                                      case sext:decode(EncValue) of
                                          {?TTL_TO_KEY_PFX, Expires, RecKey} when Expires < Now ->
                                              try
                                                  Rec = case TtlModFun of
                                                            undefined ->
                                                                null;
                                                            TtlModFun ->
                                                                ok = mfdb_lib:wait(erlfdb:add_read_conflict_key(Tx, RecEncKey)),
                                                                ok = mfdb_lib:wait(erlfdb:add_write_conflict_key(Tx, RecEncKey)),
                                                                %% get the record
                                                                RecEncKey = mfdb_lib:encode_key(TabPfx0, {?DATA_PREFIX, RecKey}),
                                                                case mfdb_lib:wait(erlfdb:get(Tx, RecEncKey)) of
                                                                    not_found ->
                                                                        null;
                                                                    EncRecVal ->
                                                                        mfdb_lib:decode_val(Tx, TabPfx0, EncRecVal)
                                                                end
                                                        end,
                                                  _ = mfdb_lib:delete(St#st{db = Tx}, RecKey),
                                                  %% Key2Ttl have to be removed individually
                                                  TtlK2T = mfdb_lib:encode_key(TabPfx, {?KEY_TO_TTL_PFX, RecKey}),
                                                  ok = mfdb_lib:wait(erlfdb:clear(Tx, TtlK2T)),
                                                  ok = mfdb_lib:wait(erlfdb:clear(Tx, EncKey)),
                                                  case {Rec, TtlModFun} of
                                                      {null, _} ->
                                                          IAcc;
                                                      {Rec, {_Mod, _Fun}} ->
                                                          [Rec | IAcc]
                                                  end
                                              catch
                                                  _E:_M:_Stack ->
                                                      io:format("CRASH: ~p~n", [{_E, _M, _Stack}]),
                                                      IAcc
                                              end;
                                          _ ->
                                              IAcc
                                      end
                              end, [], KVs),
                  {ICbRecs, length(KVs)}
          end),
    case {CbRecs, TtlModFun} of
        {[], _} ->
            0;
        {CbRecs, {Mod, Fun}} ->
            PidRefs = split_and_spawn_callbacks(Mod, Fun, CbRecs),
            ok = monitor_callbacks(PidRefs),
            length(CbRecs)
    end.

split_and_spawn_callbacks(Mod, Fun, CBRecs) ->
    split_and_spawn_callbacks(Mod, Fun, CBRecs, 0, [], []).

split_and_spawn_callbacks(Mod, Fun, [], _Cnt, Acc, PidRefs) ->
    PidRef = spawn_monitor(fun() -> [ok = Mod:Fun(R) || R <- Acc] end),
    lists:reverse([PidRef | PidRefs]);
split_and_spawn_callbacks(Mod, Fun, [Rec | Rest], Cnt, Acc, PidRefs) when Cnt < ?REAP_CALLBACK_PER_PROCESS ->
    split_and_spawn_callbacks(Mod, Fun, Rest, Cnt + 1, [Rec | Acc], PidRefs);
split_and_spawn_callbacks(Mod, Fun, [Rec | Rest], _Cnt, Acc, PidRefs) ->
    PidRef = spawn_monitor(fun() -> [ok = Mod:Fun(R) || R <- Acc] end),
    split_and_spawn_callbacks(Mod, Fun, Rest, 0, [Rec], [PidRef | PidRefs]).

monitor_callbacks([]) ->
    ok;
monitor_callbacks(PidRefs) ->
    receive
        {'DOWN', Ref, process, _Pid, normal} ->
            monitor_callbacks(lists:keydelete(Ref, 2, PidRefs))
    after
        30000 ->
            error_logger:error_msg("REAPER CALLBACK TIMEOUT!!", []),
            monitor_callbacks(tl(PidRefs))
    end.

test_callback(Record) ->
    file:write_file("/tmp/" ++ atom_to_list(node()) ++ ".log", io_lib:format("~p~n",[element(2,Record)]), [append]),
    io:format("REAPER TEST: ~p~n", [Record]),
    ok.

%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
