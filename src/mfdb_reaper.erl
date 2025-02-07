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

-export([debug/1,
         do_reap/1,
         do_reap/2]).

%% gen_server API
-export([start_link/1,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("mfdb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-else.
-define(debugFmt(F,A), ok).
-endif.

-include_lib("kernel/include/logger.hrl").
-define(REAP_POLL_INTERVAL, 5000).
-define(REAP_SEGMENT_SIZE, 200).
-define(REAP_CALLBACK_PER_PROCESS, 10).
-define(ndbg(Dbg, Fmt, Args), case Dbg of true -> ?LOG_INFO(Fmt, Args); _ -> ok end).

debug(Table) ->
    gen_server:call(?REAPERPROC(Table), debug).

do_reap(Table) ->
    do_reap(Table, #{debug => false, segment_size => ?REAP_SEGMENT_SIZE}).

do_reap(Table, Opts) when is_map(Opts) ->
    gen_server:call(?REAPERPROC(Table), {do_reap, Table, Opts});
do_reap(_Tab, _Opts) ->
    {error, <<"Call as do_reap(Table :: atom(), Opts :: #{debug := boolean(), segment_size := pos_integer()}).">>}.

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
handle_call({do_reap, Table, Opts}, _From, #{table := Table} = State) ->
    Debug = maps:get(debug, Opts, false),
    SegmentSize = maps:get(segment_size, Opts, ?REAP_SEGMENT_SIZE),
    Pid = spawn(fun() -> reap_expired_loop_(Table, SegmentSize, Debug, 0) end),
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
    try inside_reap_window() andalso reap_expired_(Table, ?REAP_SEGMENT_SIZE) of
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
            error_logger:error_msg("poller crash: ~p ~p ~p", [E,M,St]),
            {noreply, State}
    end;
handle_info(poll, #{table := Table, poller := Poller} = State) ->
    %% Non-blocking reaping of expired records from table
    Debug = maps:get(debug, State, false),
    try inside_reap_window() andalso reap_expired_(Table, ?REAP_SEGMENT_SIZE) of
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
            error_logger:error_msg("poller crash: ~p ~p ~p", [E,M,St]),
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
reap_expired_loop_(Table, SegmentSize, Debug, Total) ->
    case reap_expired_(Table, SegmentSize) of
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
            reap_expired_loop_(Table, SegmentSize, Debug, Total + Count)
    end.

%% @private
reap_expired_(Table, SegmentSize) ->
    #st{pfx = TabPfx, ttl = Ttl} = St = mfdb_manager:st(Table),
    case Ttl of
        undefined ->
            0;
        _ ->
            Now = erlang:universaltime(),
            RangeStart = mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, ?FDB_WC, ?FDB_WC}),
            RangeEnd = erlfdb_key:strinc(mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, Now, ?FDB_END})),
            reap_expired_(St, RangeStart, RangeEnd, Now, SegmentSize)
    end.

%% @private
reap_expired_(#st{db = Db, pfx = TabPfx0} = St, RangeStart, RangeEnd, Now, SegmentSize) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              KVs = mfdb_lib:wait(erlfdb:get_range(Tx, RangeStart, RangeEnd, [{limit, SegmentSize}])),
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
                      0;
                  LastKey ->
                      Count = length(KVs),
                      mfdb_lib:wait(erlfdb:clear_range(Tx, RangeStart, erlfdb_key:strinc(LastKey))),
                      Count
              end
      end).

%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec inside_reap_window() -> boolean().
inside_reap_window() ->
    case {application:get_env(mfdb, reap_window_min, undefined),
          application:get_env(mfdb, reap_window_max, undefined)} of
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
