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

-module(mfdb).

-behaviour(gen_server).

%% API
-export([connect/1]).

-export([create_table/2,
         delete_table/1,
         clear_table/1,
         table_info/2,
         table_list/0,
         import/2]).

-export([insert/2]).

-export([set_counter/3,
         update_counter/3]).

-export([delete/2]).

-export([lookup/2]).

-export([select/2,
         select/1]).

-export([index_read/3]).

-export([fold/3,
         fold/4]).

-export([subscribe/3,
         unsubscribe/2]).

-export([status/0]).

%% This is really just here to prevent dialyzer
%% from complaining about the Limit match clauses
%% for non-50 value cases
-export([do_select_/3]).

%% gen_server API
-export([start_link/1,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("mfdb.hrl").
-define(REAP_POLL_INTERVAL, 500).

%% @doc Starts the management server for a table
-spec connect(Table :: table_name()) -> ok | {error, no_such_table}.
connect(Table) when is_atom(Table) ->
    gen_server:call(mfdb_manager, {connect, Table}).

%% @doc
%% Create a new table if it does not exist
%% @end
-spec create_table(Table :: table_name(), Options :: options()) -> ok | {error, any()}.
create_table(Table, Options) when is_atom(Table) ->
    gen_server:call(mfdb_manager, {create_table, Table, Options}).

%% @doc Delete all components and data of a table
-spec delete_table(Table :: table_name()) -> ok | {error, not_connected}.
delete_table(Table) when is_atom(Table) ->
    try gen_server:call(?TABPROC(Table), stop, infinity),
         gen_server:call(mfdb_manager, {delete_table, Table})
    catch
        exit:{noproc, _}:_Stack ->
            {error, not_connected}
    end.

%% @doc Delete all records from a table
-spec clear_table(Table :: table_name()) -> ok | {error, not_connected}.
clear_table(Table) when is_atom(Table) ->
    try gen_server:call(?TABPROC(Table), clear_table, infinity)
    catch
        exit:{noproc, _}:_Stack ->
            {error, not_connected}
    end.

%% @doc import all records from SourceFile into a table.
-spec import(Table :: table_name(), SourceFile :: list()) -> ok | {error, not_connected}.
import(Table, SourceFile) when is_atom(Table) ->
    try gen_server:call(?TABPROC(Table), {import, SourceFile}, infinity)
    catch
        exit:{noproc, _}:_Stack ->
            {error, not_connected}
    end.

%% @doc get info about a table
-spec table_info(Table :: table_name(), InfoOpt :: info_opt()) ->
                        {ok, integer() | list({count | size, integer()})} | {error, not_connected}.
table_info(Table, InfoOpt)
  when is_atom(Table) andalso
       (InfoOpt =:= all orelse
        InfoOpt =:= size orelse
        InfoOpt =:= count) ->
    try gen_server:call(?TABPROC(Table), {table_info, InfoOpt})
    catch
        exit:{noproc, _}:_Stack ->
            {error, not_connected}
    end.

%% @doc List all tables
-spec table_list() -> {ok, [] | list(atom())} | {error, no_tables}.
table_list() ->
    gen_server:call(mfdb_manager, table_list).

%% @doc
%% A mnesia-style select returning a continuation()
%% Returns are chunked into buckets of 50 results
%% @end
-spec select(Table :: table_name(), Matchspec :: ets:match_spec()) ->  {[] | list(any()), continuation() | '$end_of_table'}.
select(Table, Matchspec) when is_atom(Table) ->
    select_(Table, Matchspec, 50).

%% @doc Select continuation
-spec select(Cont :: continuation()) -> {[] | list(any()), continuation() | '$end_of_table'}.
select(Cont) ->
    case Cont of
        {_, '$end_of_table'} ->
            {[], '$end_of_table'};
        {_, Cont1} when is_function(Cont1) ->
            Cont1();
        '$end_of_table' ->
            {[], '$end_of_table'};
        Cont when is_function(Cont) ->
            Cont()
    end.

%% @private
do_select_(Table, Matchspec, Limit) when is_atom(Table) ->
    select_(Table, Matchspec, Limit).

%% @doc Lookup a record by Primary Key
-spec lookup(Table :: table_name(), PkValue :: any()) -> {ok, Object :: any()} | {error, no_results}.
lookup(Table, PkValue) when is_atom(Table) ->
    #st{db = Db, pfx = TblPfx} = mfdb_manager:st(Table),
    EncKey = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, PkValue}),
    case erlfdb:get(Db, EncKey) of
        not_found ->
            {error, no_results};
        EncVal ->
            DecodedVal = mfdb_lib:decode_val(Db, TblPfx, EncVal),
            {ok, DecodedVal}
    end.

%% @doc Look up records with specific index value and returns any matching objects
-spec index_read(Table :: table_name(), IdxValue :: any(), IdxPosition :: pos_integer()) -> [] | [Object :: any()] | {error, no_index_on_field}.
index_read(Table, IdxValue, IdxPosition) when is_atom(Table) ->
    #st{index = Indexes, record_name = RName, fields = Fields} = mfdb_manager:st(Table),
    case element(IdxPosition, Indexes) of
        undefined ->
            {error, no_index_on_field};
        #idx{} ->
            %% Create a Matchspec for the specific value and use fold/select
            AccFun = fun(R, InAcc) -> [R | InAcc] end,
            Placeholder = erlang:make_tuple(length(Fields), '_'),
            MatchRec = [RName | tuple_to_list(setelement(IdxPosition - 1, Placeholder, '$1'))],
            MatchSpec = [{list_to_tuple(MatchRec),[{'=:=', '$1', IdxValue}],['$_']}],
            fold(Table, AccFun, [], MatchSpec)
    end.

%% @doc insert/replace a record. Types of values are checked if table has typed fields.
-spec insert(Table :: table_name(), ObjectTuple :: tuple()) -> ok | {error, any()}.
insert(Table, ObjectTuple) when is_atom(Table) andalso is_tuple(ObjectTuple) ->
    #st{record_name = RecName, fields = Fields, index = Index} = St = mfdb_manager:st(Table),
    IndexList = tl(tuple_to_list(Index)),
    ExpectLength = length(Fields) + 1,
    TypeCheckFun = case lists:all(fun(F) -> is_atom(F) end, Fields) of
                       true ->
                           fun(_,_) -> true end;
                       false ->
                           fun mfdb_lib:check_field_types/2
                   end,
    IndexCheckFun = case lists:all(fun(F) -> F =:= undefined end, tuple_to_list(Index)) of
                        true ->
                            fun(_,_) -> true end;
                        false ->
                            fun mfdb_lib:check_index_sizes/2
                    end,
    [RName | ObjectList] = tuple_to_list(ObjectTuple),
    [RKey | _] = ObjectList,
    InRec = {RName, size(ObjectTuple)},
    Expect = {RecName, ExpectLength},
    %% Functions must return 'true' to continue, anything else will exit early
    Flow = [{fun(X,Y) -> X =:= Y orelse {error, invalid_record} end, [InRec, Expect]},
            {TypeCheckFun, [ObjectList, Fields]},
            {IndexCheckFun, [ObjectList, IndexList]},
            {fun mfdb_lib:put/3, [St, RKey, ObjectTuple]}],
    mfdb_lib:flow(Flow, true).

%% @doc Atomic counter increment/decrement
-spec update_counter(Table :: table_name(), Key :: any(), Increment :: integer()) -> integer().
update_counter(Table, Key, Increment) when is_atom(Table) andalso is_integer(Increment) ->
    #st{db = Db, pfx = TabPfx} = mfdb_manager:st(Table),
    mfdb_lib:update_counter(Db, TabPfx, Key, Increment).

%% @doc Atomic set of a counter value
-spec set_counter(Table :: table_name(), Key :: any(), Value :: integer()) -> ok.
set_counter(Table, Key, Value) when is_atom(Table) andalso is_integer(Value) ->
    #st{db = Db, pfx = TabPfx} = mfdb_manager:st(Table),
    mfdb_lib:set_counter(Db, TabPfx, Key, Value).

%% @doc Applies a function to all records in the table
-spec fold(Table :: table_name(), InnerFun :: function(), OuterAcc :: any()) ->  any().
fold(Table, InnerFun, OuterAcc) ->
    MatchSpec = [{'_',[],['$_']}],
    fold_cont_(select(Table, MatchSpec), InnerFun, OuterAcc).

%% @doc Applies a function to all records in the table matching the MatchSpec
-spec fold(Table :: table_name(), InnerFun :: function(), OuterAcc :: any(), MatchSpec :: ets:match_spec()) ->  any().
fold(Table, InnerFun, OuterAcc, MatchSpec) ->
    fold_cont_(select(Table, MatchSpec), InnerFun, OuterAcc).

%% @doc Delete a record from the table
-spec delete(Table :: table_name(), PkVal :: any()) ->  ok.
delete(Table, PkValue) when is_atom(Table) ->
    #st{} = St = mfdb_manager:st(Table),
    mfdb_lib:delete(St, PkValue).

-spec subscribe(Table :: table_name(), Key :: any(), ReplyType :: watcher_option()) -> ok | {error, invalid_reply | no_such_table | {any(), any()}}.
subscribe(Table, Key, ReplyType) when is_atom(Table) ->
    case mfdb_lib:validate_reply_(ReplyType) of
        true ->
            try gen_server:call(?TABPROC(Table), {subscribe, ReplyType, Key}, infinity)
            catch
                exit:{noproc, _} ->
                    {error, no_such_table};
                E:M ->
                    {error, {E,M}}
            end;
        false ->
            {error, invalid_reply}
    end.

-spec unsubscribe(Table :: table_name(), any()) -> ok | {error, no_such_table | {any(), any()}}.
unsubscribe(Table, Key) when is_atom(Table) ->
    try gen_server:call(?TABPROC(Table), {unsubscribe, Key}, infinity)
    catch
        exit:{noproc, _} ->
            {error, no_such_table};
        E:M:_St ->
            {error, {E,M}}
    end.

%% @doc return FoundDB cluster status information as a JSON term
%% @see https://apple.github.io/foundationdb/mr-status.html
-spec status() -> jsx:json_term().
status() ->
    Db = mfdb_conn:connection(),
    jsx:decode(erlfdb:get(Db, <<16#FF,16#FF,"/status/json">>), [{return_maps, false}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%% @private
start_link(Table) ->
    gen_server:start_link(?TABPROC(Table), ?MODULE, [Table], []).

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

%% @private
handle_call(clear_table, _From, #{table := Table} = State) ->
    %% potentially long-running operation. Caller waits 'infinity'
    #st{db = Db, pfx = TblPfx, index = Indexes} = mfdb_manager:st(Table),
    mfdb_lib:clear_table(Db, TblPfx, Indexes),
    {reply, ok, State};
handle_call({import, SourceFile}, From, #{table := Table} = State) ->
    %% potentially long-running operation. Caller waits 'infinity'
    ImportId = erlang:make_ref(),
    {Pid, Ref} = spawn_monitor(fun() -> do_import_(Table, SourceFile, ImportId) end),
    OldWaiters = maps:get(waiters, State, []),
    NewWaiters = [{ImportId, Pid, Ref, From} | OldWaiters],
    {noreply, State#{waiters => NewWaiters}};
handle_call({table_info, all}, _From, #{table := Table} = State) ->
    #st{} = St = mfdb_manager:st(Table),
    Count = mfdb_lib:table_count(St),
    Size = mfdb_lib:table_data_size(St),
    {reply, {ok, [{count, Count}, {size, Size}]}, State};
handle_call({table_info, count}, _From, #{table := Table} = State) ->
    #st{} = St = mfdb_manager:st(Table),
    Count = mfdb_lib:table_count(St),
    {reply, {ok, Count}, State};
handle_call({table_info, size}, _From, #{table := Table} = State) ->
    #st{} = St = mfdb_manager:st(Table),
    Size = mfdb_lib:table_data_size(St),
    {reply, {ok, Size}, State};
handle_call({subscribe, ReplyType, Key}, From, #{table := Table} = State) ->
    #st{pfx = TblPfx, tab = Tab} = mfdb_manager:st(Table),
    Reply = key_subscribe_(Tab, ReplyType, From, TblPfx, Key),
    {reply, Reply, State};
handle_call({unsubscribe, Key}, From, #{table := Table} = State) ->
    #st{pfx = Prefix} = mfdb_manager:st(Table),
    Reply = key_unsubscribe_(From, Prefix, Key),
    {reply, Reply, State};
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
            PidRef = spawn_monitor(fun() -> reap_expired_(Table) end),
            {noreply, State#{poller => poll_timer(Poller), reaper => PidRef}};
        _ ->
            {noreply, State#{poller => poll_timer(Poller)}}
    end;
handle_info({'DOWN', Ref, process, _Pid0, {normal, {ImportId, {ok, X}}}}, #{waiters := Waiters} = State) ->
    %% Import is completed
    case lists:keytake(ImportId, 1, Waiters) of
        false ->
            error_logger:error_msg("Missing waiter for import ~p", [ImportId]),
            {noreply, State};
        {value, {ImportId, _Pid1, Ref, From}, NWaiters} ->
            demonitor(Ref),
            gen_server:reply(From, {ok, X}),
            {noreply, State#{waiters => NWaiters}}
    end;
handle_info({'DOWN', Ref, process, _Pid0, {normal, {ImportId, ImportError}}}, #{waiters := Waiters} = State) ->
    %% Import is completed
    case lists:keytake(ImportId, 1, Waiters) of
        false ->
            error_logger:error_msg("Missing waiter for import ~p", [ImportId]),
            {noreply, State};
        {value, {ImportId, _Pid1, Ref, From}, NWaiters} ->
            demonitor(Ref),
            gen_server:reply(From, ImportError),
            {noreply, State#{waiters => NWaiters}}
    end;
handle_info({'DOWN', Ref, process, _Pid0, _Reason}, #{poller := Poller} = State) ->
    case maps:get(reaper, State, undefined) of
        undefined ->
            {noreply, State#{poller => poll_timer(Poller)}};
        {_Pid1, Ref} ->
            erlang:demonitor(Ref),
            {noreply, State#{poller => poll_timer(Poller), reaper => undefined}};
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
poll_timer(undefined) ->
    erlang:send_after(?REAP_POLL_INTERVAL, self(), poll);
poll_timer(TRef) when is_reference(TRef) ->
    erlang:cancel_timer(TRef),
    erlang:send_after(?REAP_POLL_INTERVAL, self(), poll).

%% @private
key_subscribe_(Table, ReplyType, From, TblPfx, Key) ->
    try mfdb_watcher:subscribe(ReplyType, From, TblPfx, Key)
    catch
        exit:{noproc, _} ->
            %% No watcher process exists for the Key
            ok = mfdb_watcher_sup:create(Table, TblPfx, Key),
            key_subscribe_(Table, ReplyType, From, TblPfx, Key);
        E:M:_St ->
            {error, {E,M}}
    end.

%% @private
key_unsubscribe_(From, Prefix, Key) ->
    try mfdb_watcher:unsubscribe(From, Prefix, Key)
    catch
        exit:{noproc, _} ->
            ok;
        E:M:_St ->
            {error, {E,M}}
    end.

%% @private
reap_expired_(Table) ->
    #st{pfx = TabPfx, ttl = Ttl} = St = mfdb_manager:st(Table),
    case mfdb_lib:expired(Ttl) of
        never ->
            %% No expiration
            ok;
        Expired ->
            RangeStart = mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, ?FDB_WC, ?FDB_WC}),
            RangeEnd = mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, Expired, ?FDB_END}),
            reap_expired_(St, RangeStart, RangeEnd)
    end.

%% @private
reap_expired_(#st{db = Db, pfx = TabPfx} = St, RangeStart, RangeEnd) ->
    Tx = erlfdb:create_transaction(Db),
    try erlfdb:wait(erlfdb:get_range(Tx, RangeStart, erlfdb_key:strinc(RangeEnd), [{limit, 1000}])) of
        [] ->
            ok;
        KVs ->
            LastKey = lists:foldl(
                        fun({EncKey, <<>>}, _) ->
                                %% Delete the actual expired record
                                RecKey = mfdb_lib:decode_key(TabPfx, EncKey),
                                ok = mfdb_lib:delete(St#st{db = Tx}, RecKey),
                                %% Key2Ttl have to be removed individually
                                TtlK2T = mfdb_lib:encode_key(TabPfx, {?KEY_TO_TTL_PFX, RecKey}),
                                ok = erlfdb:wait(erlfdb:clear(Tx, TtlK2T)),
                                EncKey
                        end, ok, KVs),
            ok = erlfdb:wait(erlfdb:clear_range(Tx, RangeStart, erlfdb_key:strinc(LastKey))),
            ok = erlfdb:wait(erlfdb:commit(Tx))
    catch
        _E:_M:_Stack ->
            error_logger:error_msg("Reaping error: ~p", [{_E, _M, _Stack}]),
            ok
    end.

%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%% INTERNAL %%%%%%%%%%%%%%%%%%%%%%%

%% @private
select_(Table, Matchspec0, Limit) when is_atom(Table) ->
    #st{db = Db, pfx = TabPfx, index = Indexes0} = St = mfdb_manager:st(Table),
    {Guards, Binds, Ms} =
        case Matchspec0 of
            [{ {{_V, '$1'}} , G, _}] ->
                {G, [{1, ['$1']}], Matchspec0};
            [{HP, G, MsRet}] ->
                {NewHP, NewGuards} = ms_rewrite(HP, G),
                NewMs = [{NewHP, NewGuards, MsRet}],
                {NewGuards, bound_in_headpat(NewHP), NewMs};
            _ ->
                {[], [], Matchspec0}
        end,
    Indexes = [I || #idx{} = I <- tuple_to_list(Indexes0)],
    RangeGuards = range_guards(Guards, Binds, Indexes, []),
    {PkStart, PkEnd} = primary_table_range_(RangeGuards),
    case idx_sel(RangeGuards, Indexes, St) of
        {use_index, IdxParams} = _IdxSel ->
            do_indexed_select(Table, Ms, IdxParams, Limit);
        no_index ->
            do_select(Db, TabPfx, Ms, PkStart, PkEnd, Limit)
    end.

%% @private
fold_cont_({[], '$end_of_table'}, _InnerFun, OuterAcc) ->
    OuterAcc;
fold_cont_({Data, '$end_of_table'}, InnerFun, OuterAcc) ->
    lists:foldl(InnerFun, OuterAcc, Data);
fold_cont_({Data, Cont}, InnerFun, OuterAcc) when is_function(Cont) ->
    NewAcc = lists:foldl(InnerFun, OuterAcc, Data),
    fold_cont_(Cont(), InnerFun, NewAcc).

%% @private
is_wild('_') ->
    true;
is_wild(A) when is_atom(A) ->
    case atom_to_list(A) of
        "\$" ++ S ->
            try begin
                    _ = list_to_integer(S),
                    true
                end
            catch
                error:_ ->
                    false
            end;
        _ ->
            false
    end.

%% @private
bound_in_headpat(HP) when is_atom(HP) ->
    {all, HP};
bound_in_headpat(HP) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    map_vars(T, 2);
bound_in_headpat(_) ->
    %% this is not the place to throw an exception
    none.

%% @private
map_vars([H|T], P) ->
    case extract_vars(H) of
        [] ->
            map_vars(T, P+1);
        Vs ->
            [{P, Vs}|map_vars(T, P+1)]
    end;
map_vars([], _) ->
    [].

%% @private
ms_rewrite(HP, Guards) when is_atom(HP) ->
    {HP, Guards};
ms_rewrite(HP, Guards) when is_tuple(HP) ->
    [_|T] = tuple_to_list(HP),
    {_, Used, Matches} =
        lists:foldl(
          fun('_', {Inc, U, Acc}) ->
                  {Inc + 1, U, Acc};
             (M, {Inc, U, Acc}) when is_atom(M) ->
                  case atom_to_binary(M, utf8) of
                      <<"\$", _/binary>> ->
                          {Inc + 1, [M | U], Acc};
                      _ ->
                          {Inc + 1, U, [{Inc, M} | Acc]}
                  end;
             (Match, {Inc, U, Acc}) ->
                  {Inc + 1, U, [{Inc, Match} | Acc]}
          end,
          {2, [], []}, T),
    headpat_matches_to_guards(HP, Guards, Used, Matches).

%% @private
headpat_matches_to_guards(HP, Guards, _Used, []) ->
    {HP, Guards};
headpat_matches_to_guards(HP, Guards, Used0, [{Idx, Val} | Rest]) ->
    {Used, Bind} = next_bind(Used0, Idx),
    NewHP = setelement(Idx, HP, Bind),
    NewGuards = [{'=:=', Bind, Val} | Guards],
    headpat_matches_to_guards(NewHP, NewGuards, Used, Rest).

%% @private
next_bind(Used, X) ->
    Bind = list_to_atom("\$" ++ integer_to_list(X)),
    case lists:member(Bind, Used) of
        true ->
            next_bind(Used, X+1);
        false ->
            {[Bind | Used], Bind}
    end.

%% @private
-spec range_guards(list({atom(), atom(), any()}), list(), list(), list()) -> list().
range_guards([], _Binds, _Indexes, Acc) ->
    lists:keysort(1, Acc);
range_guards([{Comp, Bind, Val} | Rest], Binds, Indexes, Acc) ->
    case lists:member(Comp, ['>', '>=', '<', '=<', '=:=']) of
        true ->
            case lists:keyfind([Bind], 2, Binds) of
                false ->
                    range_guards(Rest, Binds, Indexes, Acc);
                {Idx, _} ->
                    case Idx =:= 2 orelse lists:keyfind(Idx, #idx.pos, Indexes) of
                        true ->
                            %% Column indexed
                            range_guards(Rest, Binds, Indexes, [{Idx, Comp, Val} | Acc]);
                        false ->
                            %% Column not indexed
                            range_guards(Rest, Binds, Indexes, Acc);
                        #idx{} ->
                            range_guards(Rest, Binds, Indexes, [{Idx, Comp, Val} | Acc])
                    end
            end;
        false ->
            range_guards(Rest, Binds, Indexes, Acc)
    end.


%% @private
primary_table_range_(Guards) ->
    primary_table_range_(Guards, undefined, undefined).

%% @private
primary_table_range_([], Start, End) ->
    {replace_(Start, ?FDB_WC), replace_(End, ?FDB_END)};
primary_table_range_([{2, '>=', V} | Rest], undefined, End) ->
    primary_table_range_(Rest, {gte, V}, End);
primary_table_range_([{2, '>', V} | Rest], undefined, End) ->
    primary_table_range_(Rest, {gt, V}, End);
primary_table_range_([{2, '=<', V} | Rest], Start, undefined) ->
    primary_table_range_(Rest, Start, {lte, V});
primary_table_range_([{2, '<', V} | Rest], Start, undefined) ->
    primary_table_range_(Rest, Start, {lt, V});
primary_table_range_([_ | Rest], Start, End) ->
    primary_table_range_(Rest, Start, End).

%% @private
replace_(undefined, Val) ->
    Val;
replace_(Orig, _Val) ->
    Orig.

%% @private
idx_sel(Guards, Indexes, #st{pfx = TabPfx} = St0) ->
    IdxSel0 = [begin
                   #idx{pos = KeyPos, data_key = IdxDataPfx} = Idx,
                   I = idx_table_params_(Guards, KeyPos, TabPfx, IdxDataPfx),
                   IdxValCount = case I of
                                     {_, _, {{'$1', '$2'}}, _} ->
                                         undefined;
                                     {_, _, {{M, '$2'}}, _} ->
                                         %% we have an exact match on in indexed value
                                         mfdb_lib:idx_matches(St0, KeyPos, M);
                                     _ ->
                                         undefined
                                 end,
                   {KeyPos, I, IdxValCount}
               end || Idx <- Indexes],
    case IdxSel0 of
        [] ->
            no_index;
        IdxSel0 ->
            AvailIdx = [{idx_val_(I), Kp, I, IdxVCount} || {Kp, I, IdxVCount} <- IdxSel0, I =/= undefined],
            case idx_pick(AvailIdx) of
                undefined ->
                    no_index;
                {_, IdxId, IdxSel, _} ->
                    {use_index, {IdxId, IdxSel}}
            end
    end.

%% @private
idx_pick(Idx) ->
    idx_pick(Idx, undefined).

%% @private
idx_pick([], Res) ->
    Res;
idx_pick([First | Rest], undefined) ->
    idx_pick(Rest, First);
idx_pick([{_IdxVal0, _, _, ValCount0} = Idx | Rest], {_IdxVal1, _, _, ValCount1})
  when is_integer(ValCount0) andalso
       is_integer(ValCount1) andalso
       ValCount0 < ValCount1 ->
    %% less keys to scan through
    idx_pick(Rest, Idx);
idx_pick([{_IdxVal0, _, _, ValCount0} | Rest], {_IdxVal1, _, _, ValCount1} = Idx)
  when is_integer(ValCount0) andalso
       is_integer(ValCount1) andalso
       ValCount1 < ValCount0 ->
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, ValCount0} | Rest], {IdxVal1, _, _, ValCount1} = Idx)
  when is_integer(ValCount0) andalso
       is_integer(ValCount1) andalso
       ValCount0 =:= ValCount1 andalso
       IdxVal1 >= IdxVal0 ->
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, ValCount0} = Idx | Rest], {IdxVal1, _, _, ValCount1})
  when is_integer(ValCount0) andalso
       is_integer(ValCount1) andalso
       ValCount0 =:= ValCount1 andalso
       IdxVal0 >= IdxVal1 ->
    idx_pick(Rest, Idx);
idx_pick([{_IdxVal0, _, _, ValCount0} = Idx | Rest], {_IdxVal1, _, _, undefined})
  when is_integer(ValCount0) ->
    %% explicit index vs scan
    idx_pick(Rest, Idx);
idx_pick([{_IdxVal0, _, _, undefined} | Rest], {_IdxVal1, _, _, ValCount1} = Idx)
  when is_integer(ValCount1) ->
    %% explicit index vs scan
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, undefined} = Idx | Rest], {IdxVal1, _, _, undefined})
  when IdxVal0 >= IdxVal1 ->
    %% explicit index vs scan
    idx_pick(Rest, Idx);
idx_pick([{IdxVal0, _, _, undefined} | Rest], {IdxVal1, _, _, undefined} = Idx)
  when IdxVal1 >= IdxVal0 ->
    idx_pick(Rest, Idx).

%% @doc convert guards into index-table specific selectors
%% Guards are the guards extracted from the MatchSpec.
%% This is very basic/naive implementation
%% @todo :: deal with multi-conditional guards with 'andalso', 'orelse' etc
%% @end
%% @private
idx_table_params_(Guards, Keypos, TblPfx, IdxDataPfx) ->
    case lists:keyfind(Keypos, 1, Guards) of
        false ->
            undefined;
        _ ->
            idx_table_params_(Guards, Keypos, TblPfx, IdxDataPfx, undefined, undefined, {{'$1', '$2'}}, [])
    end.

%% @private
idx_table_params_([], _Keypos, TblPfx, IdxDataPfx, Start, End, Match, Guards) ->
    PfxStart = index_pfx(IdxDataPfx, start, ?FDB_WC, true),
    PfxEnd = index_pfx(IdxDataPfx, 'end', ?FDB_END, true),
    {replace_(Start, {fdb, mfdb_lib:encode_prefix(TblPfx, PfxStart)}),
     replace_(End, {fdb, erlfdb_key:strinc(mfdb_lib:encode_prefix(TblPfx, PfxEnd))}),
     Match,
     Guards};
idx_table_params_([{Keypos, '=:=', Val} | Rest], Keypos, TblPfx, IdxDataPfx, _Start, _End, _Match, Guards) ->
    Match = {{Val, '$2'}},
    PfxStart = index_pfx(IdxDataPfx, start, Val, true),
    PfxEnd = index_pfx(IdxDataPfx, 'end', Val, true),
    Start = {fdbr, mfdb_lib:encode_prefix(TblPfx, PfxStart)},
    End = {fdbr, erlfdb_key:strinc(mfdb_lib:encode_prefix(TblPfx, PfxEnd))},
    idx_table_params_(Rest, Keypos, TblPfx, IdxDataPfx, Start, End, Match, Guards);
idx_table_params_([{Keypos, Comp, Val} | Rest], Keypos, TblPfx, IdxDataPfx, Start, End, Match, Guards)
  when Comp =:= '>=' orelse Comp =:= '>' ->
    NGuards = [{Comp, '$1', Val} | Guards],
    PfxStart = index_pfx(IdxDataPfx, start, Val, true),
    NStart0 = {fdbr, mfdb_lib:encode_prefix(TblPfx, PfxStart)},
    NStart = replace_(Start, NStart0),
    idx_table_params_(Rest, Keypos, TblPfx, IdxDataPfx, NStart, End, Match, NGuards);
idx_table_params_([{Keypos, Comp, Val} | Rest], Keypos, TblPfx, IdxDataPfx, Start, End, Match, Guards)
  when Comp =:= '=<' orelse Comp =:= '<' ->
    NGuards = [{Comp, '$1', Val} | Guards],
    PfxEnd = index_pfx(IdxDataPfx, 'end', Val, true),
    NEnd0 = {fdbr, erlfdb_key:strinc(mfdb_lib:encode_prefix(TblPfx, PfxEnd))},
    NEnd = replace_(End, NEnd0),
    idx_table_params_(Rest, Keypos, TblPfx, IdxDataPfx, Start, NEnd, Match, NGuards);
idx_table_params_([_ | Rest], Keypos, TblPfx, IdxDataPfx, Start, End, Match, Guards) ->
    idx_table_params_(Rest, Keypos, TblPfx, IdxDataPfx, Start, End, Match, Guards).

%% @private
index_pfx(IdxDataPfx, start, V, true) ->
    Pfx = {IdxDataPfx, {V, ?FDB_WC}},
    Pfx;
index_pfx(IdxDataPfx, 'end', V, true) ->
    Pfx = {IdxDataPfx, {V, ?FDB_END}},
    Pfx.

%% @private
v_(undefined) -> 0;
v_({fdb, B}) when is_binary(B) -> 1; %% pre-calculated range
v_({fdbr, B}) when is_binary(B) -> 2; %% pre-calculated range
v_({{_, '$1'}}) -> 10;
v_({{'$1', '$2'}}) -> 0; %% indicates guards will be processed
v_({{_, '$2'}}) -> 20; %% indicates a head-bound match
v_({_, _}) -> 1;
v_(?FDB_WC) -> 0; %% scan from start of table
v_(?FDB_END) -> 0; %% scan to end of table
v_(B) when is_binary(B) -> 1;
v_(L) when is_list(L) ->
    %% number of guards
    length(L) * 0.5.

%% @private
idx_val_(undefined) ->
    0;
idx_val_({S,E,C,G}) ->
    lists:sum([v_(X) || X <- [S,E,C,G]]).

%% @private
wild_in_body(BodyVars) ->
    intersection(BodyVars, ['$$','$_']) =/= [].

%% @private
needs_key_only([{HP,_,Body}]) ->
    BodyVars = lists:flatmap(fun extract_vars/1, Body),
    %% Note that we express the conditions for "needs more than key" and negate.
    InHead = bound_in_headpat(HP),
    not(wild_in_body(BodyVars) orelse
        case InHead of
            {all,V} -> lists:member(V, BodyVars);
            none    -> false;
            Vars    -> any_in_body(lists:keydelete(2,1,Vars), BodyVars)
        end);
needs_key_only(_) ->
    %% don't know
    false.

%% @private
any_in_body(Vars, BodyVars) ->
    lists:any(fun({_,Vs}) ->
                      intersection(Vs, BodyVars) =/= []
              end, Vars).

%% @private
extract_vars([H|T]) ->
    extract_vars(H) ++ extract_vars(T);
extract_vars(T) when is_tuple(T) ->
    extract_vars(tuple_to_list(T));
extract_vars(T) when T=='$$'; T=='$_' ->
    [T];
extract_vars(T) when is_atom(T) ->
    case is_wild(T) of
        true ->
            [T];
        false ->
            []
    end;
extract_vars(_) ->
    [].

%% @private
intersection(A,B) when is_list(A), is_list(B) ->
    A -- (A -- B).

%% @private
outer_match_fun_(TabPfx, OCompiledKeyMs) ->
    fun(Tx, Id, Acc0) ->
            K = mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, Id}),
            case erlfdb:wait(erlfdb:get(Tx, K)) of
                not_found ->
                    %% This should only happen with a dead index
                    %% entry (data deleted after we got index)
                    Acc0;
                V ->
                    Rec = mfdb_lib:decode_val(Tx, TabPfx, V),
                    case ets:match_spec_run([Rec], OCompiledKeyMs) of
                        [] ->
                            %% Did not match specification
                            Acc0;
                        [Matched] ->
                            %% Matched specification
                            [Matched | Acc0]
                    end
            end
    end.

%% @private
index_match_fun_(ICompiledKeyMs, OuterMatchFun) ->
    fun(Tx, {{_, Id}} = R, IAcc) ->
            case ets:match_spec_run([R], ICompiledKeyMs) of
                [] ->
                    %% Did not match
                    IAcc;
                [{{_, Id}}] ->
                    %% Matched
                    OuterMatchFun(Tx, Id, IAcc)
            end
    end.

%% @private
pk_to_range(TabPfx, start, {gt, X}) ->
    {fdbr, erlfdb_key:strinc(mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X}))};
pk_to_range(TabPfx, start, {gte, X}) ->
    {fdbr, mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X})};
pk_to_range(TabPfx, 'end', {lt, X}) ->
    {fdbr, mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X})};
pk_to_range(TabPfx, 'end', {lte, X}) ->
    {fdbr, erlfdb_key:strinc(mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X}))};
pk_to_range(TabPfx, start, _) ->
    {fdbr, mfdb_lib:encode_prefix(TabPfx, {?DATA_PREFIX, ?FDB_WC})};
pk_to_range(TabPfx, 'end', _) ->
    {fdbr, erlfdb_key:strinc(mfdb_lib:encode_prefix(TabPfx, {?DATA_PREFIX, ?FDB_END}))}.

%% @private
do_select(Db, TabPfx, MS, PkStart, PkEnd, Limit) ->
    KeysOnly = needs_key_only(MS),
    Keypat = case (PkStart =/= undefined orelse PkEnd =/= undefined) of
                 true  ->
                     RangeStart = pk_to_range(TabPfx, start, PkStart),
                     RangeEnd = pk_to_range(TabPfx, 'end', PkEnd),
                     {range, RangeStart, RangeEnd};
                 false ->
                     mfdb_lib:encode_prefix(TabPfx, {?DATA_PREFIX, ?FDB_WC})
             end,
    CompiledMs = ets:match_spec_compile(MS),
    DataFun = undefined,
    InAcc = [],
    Iter = iter_(Db, TabPfx, Keypat, KeysOnly, CompiledMs, DataFun, InAcc, Limit),
    do_iter(Iter, Limit, []).

%% @private
do_indexed_select(Tab0, MS, {_IdxPos, {Start, End, Match, Guard}}, Limit) ->
    #st{db = Db, pfx = TabPfx} = mfdb_manager:st(Tab0),
    KeyMs = [{Match, Guard, ['$_']}],
    OKeysOnly = needs_key_only(MS),
    ICompiledKeyMs = ets:match_spec_compile(KeyMs),
    OCompiledKeyMs = ets:match_spec_compile(MS),
    OuterMatchFun = outer_match_fun_(TabPfx, OCompiledKeyMs),
    DataFun = index_match_fun_(ICompiledKeyMs, OuterMatchFun),
    OAcc = [],
    IRange = {range, Start, End},
    Iter = iter_(Db, TabPfx, IRange, OKeysOnly, undefined, DataFun, OAcc, Limit),
    do_iter(Iter, Limit, []).

%% @private
do_iter('$end_of_table', Limit, Acc) when Limit =:= 0 ->
    {mfdb_lib:sort(Acc), '$end_of_table'};
do_iter({Data, '$end_of_table'}, Limit, Acc) when Limit =:= 0 ->
    {mfdb_lib:sort(lists:append(Acc, Data)), '$end_of_table'};
do_iter({Data, Iter}, Limit, Acc) when Limit =:= 0 andalso is_function(Iter) ->
    NAcc = mfdb_lib:sort(lists:append(Acc, Data)),
    case Iter() of
        '$end_of_table' ->
            NAcc;
        {_, '$end_of_table'} = NIter ->
            do_iter(NIter, Limit, NAcc);
        {_, ?IS_ITERATOR} = NIter ->
            do_iter(NIter, Limit, NAcc)
    end;
do_iter('$end_of_table', Limit, Acc) when Limit > 0 ->
    {mfdb_lib:sort(Acc), '$end_of_table'};
do_iter({Data, '$end_of_table'}, Limit, Acc) when Limit > 0 ->
    {mfdb_lib:sort(lists:append(Acc, Data)), '$end_of_table'};
do_iter({[], Iter}, Limit, Acc) when Limit > 0 andalso is_function(Iter) ->
    do_iter(Iter(), Limit, Acc);
do_iter({Data, Iter}, Limit, Acc) when Limit > 0 andalso is_function(Iter) ->
    NAcc = mfdb_lib:sort(lists:append(Acc, Data)),
    {NAcc, Iter}.

%% @private
-spec iter_(Db :: ?IS_DB, TabPfx :: binary(), StartKey :: any(), KeysOnly :: boolean(), Ms :: undefined | ets:comp_match_spec(), DataFun :: undefined | function(), InAcc :: list(), DataLimit :: pos_integer()) ->
                   {list(), '$end_of_table'} | {list(), ?IS_ITERATOR}.
iter_(?IS_DB = Db, TabPfx, StartKey0, KeysOnly, Ms, DataFun, InAcc, DataLimit) ->
    Reverse = 0, %% we're not iterating in reverse
    {SKey, EKey} = iter_start_end_(TabPfx, StartKey0),
    St0 = #iter_st{
             db = Db,
             pfx = TabPfx,
             data_limit = DataLimit,
             data_acc = InAcc,
             data_fun = DataFun,
             keys_only = KeysOnly,
             compiled_ms = Ms,
             start_key = SKey,
             start_sel = erlfdb_key:to_selector(SKey),
             end_sel = erlfdb_key:to_selector(EKey),
             limit = 100, %% we use a fix limit of 100 for the number of KVs to pull
             target_bytes = 0,
             streaming_mode = iterator,%% it's *not* stream_iterator bad type spec in erlfdb_nif
             iteration = 1,
             snapshot = true,
             reverse = Reverse
            },
    St = iter_transaction_(St0),
    iter_int_({cont, St}).

%% @private
iter_int_({cont, #iter_st{tx = Tx,
                          pfx = TabPfx,
                          keys_only = KeysOnly,
                          compiled_ms = Ms,
                          data_limit = DataLimit, %% Max rec in accum per continuation
                          data_count = DataCount, %% count in continuation accumulator
                          data_acc = DataAcc, %% accum for continuation
                          data_fun = DataFun, %% Fun applied to selected data when not key-only
                          iteration = Iteration} = St0}) ->
    {{RawRows, Count, HasMore0}, St} = iter_future_(St0),
    case Count of
        0 ->
            {DataAcc, '$end_of_table'};
        _ ->
            {Rows, HasMore, LastKey} = rows_more_last_(DataLimit, DataCount, RawRows,
                                                       Count, HasMore0),
            {NewDataAcc, AddCount} = iter_append_(Rows, Tx, TabPfx, KeysOnly, Ms,
                                                  DataFun, 0, DataAcc),
            NewDataCount = DataCount + AddCount,
            HitLimit = hit_data_limit_(NewDataCount, DataLimit),
            Done = RawRows =:= []
                orelse HitLimit =:= true
                orelse (HitLimit =:= false andalso HasMore =:= false),
            case Done of
                true when HasMore =:= false ->
                    %% no more rows
                    iter_commit_(Tx),
                    {lists:reverse(NewDataAcc), '$end_of_table'};
                true when HasMore =:= true ->
                    %% there are more rows, return accumulated data and a continuation fun
                    NSt0 = St#iter_st{
                             start_sel = erlfdb_key:first_greater_than(LastKey),
                             iteration = Iteration + 1,
                             data_count = 0,
                             data_acc = []
                            },
                    NSt = iter_transaction_(NSt0),
                    {lists:reverse(NewDataAcc), fun() -> iter_int_({cont, NSt}) end};
                false ->
                    %% there are more rows, but we need to continue accumulating
                    %% This loops internally, so no fun, just the continuation
                    NSt0 = St#iter_st{
                             start_sel = erlfdb_key:first_greater_than(LastKey),
                             iteration = Iteration + 1,
                             data_count = NewDataCount,
                             data_acc = NewDataAcc
                            },
                    NSt = iter_transaction_(NSt0),
                    iter_int_({cont, NSt})
            end
    end.

%% @private
iter_append_([], _Tx, _TabPfx, _KeysOnly, _Ms, _DataFun, AddCount, Acc) ->
    {Acc, AddCount};
iter_append_([{K, V} | Rest], Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount, Acc) ->
    case mfdb_lib:decode_key(TabPfx, K) of
        {idx, {{IK, IV}} = Idx} when is_function(DataFun, 3) ->
            %% Record matched specification, is a fold operation, apply the supplied DataFun
            case not lists:member({IK, IV}, Acc) of
                true ->
                    NAcc = DataFun(Tx, Idx, Acc),
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                false ->
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, Acc)
            end;
        {idx, {{IK, IV}} = Idx} ->
            case not lists:member({IK, IV}, Acc) andalso
                Ms =/= undefined andalso
                ets:match_spec_run([Idx], Ms) of
                false ->
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, Acc);
                [] ->
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, Acc);
                [Match] ->
                    NAcc = [Match | Acc],
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc)
            end;
        Key ->
            Rec = mfdb_lib:decode_val(Tx, TabPfx, V),
            case Ms =/= undefined andalso ets:match_spec_run([Rec], Ms) of
                false  when is_function(DataFun, 2) ->
                    %% Record matched specification, is a fold operation, apply the supplied DataFun
                    NAcc = DataFun(Rec, Acc),
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                false  when is_function(DataFun, 3) ->
                    %% Record matched specification, is a fold operation, apply the supplied DataFun
                    NAcc = DataFun(Tx, Rec, Acc),
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                false ->
                    NAcc = [iter_val_(KeysOnly, Key, Rec) | Acc],
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                [] ->
                    %% Record did not match specification
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount, Acc);
                [Match] when DataFun =:= undefined ->
                    %% Record matched specification, but not a fold operation
                    NAcc = [Match | Acc],
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                [Match] when is_function(DataFun, 2) ->
                    %% Record matched specification, is a fold operation, apply the supplied DataFun
                    NAcc = DataFun(Match, Acc),
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc)
            end
    end.

%% @private
iter_val_(true, Key, _Rec) ->
    Key;
iter_val_(false, _Key, Rec) ->
    Rec.

%% @private
iter_start_end_(TblPfx, StartKey0) ->
    case StartKey0 of
        {range, S0, E0} ->
            S = case S0 of
                    {S1k, S1} when S1k =:= fdb orelse S1k =:= fdbr ->
                        erlfdb_key:first_greater_or_equal(S1);
                    S0 ->
                        mfdb_lib:encode_prefix(TblPfx, {?DATA_PREFIX, S0})
                end,
            E = case E0 of
                    {E1k, E1} when E1k =:= fdb orelse E1k =:= fdbr ->
                        E1;
                    E0 ->
                        erlfdb_key:strinc(mfdb_lib:encode_prefix(TblPfx, {?DATA_PREFIX, E0}))
                end,
            {S, E};
        StartKey0 ->
            {erlfdb_key:first_greater_than(StartKey0), erlfdb_key:strinc(StartKey0)}
    end.

%% @private
iter_transaction_(#iter_st{db = Db, tx = Tx} = St) ->
    iter_commit_(Tx),
    NTx = erlfdb:create_transaction(Db),
    erlfdb_nif:transaction_set_option(NTx, disallow_writes, 1),
    St#iter_st{tx = NTx}.

%% @private
iter_commit_(undefined) ->
    ok;
iter_commit_(?IS_TX = Tx) ->
    catch erlfdb:wait(erlfdb:commit(Tx)),
    ok.

%% @private
iter_future_(#iter_st{tx = Tx, start_sel = StartKey,
                      end_sel = EndKey, limit = Limit,
                      target_bytes = TargetBytes, streaming_mode = StreamingMode,
                      iteration = Iteration, snapshot = Snapshot,
                      reverse = Reverse} = St0) ->
    try erlfdb:wait(erlfdb_nif:transaction_get_range(
                      Tx,
                      StartKey,
                      EndKey,
                      Limit,
                      TargetBytes,
                      StreamingMode,
                      Iteration,
                      Snapshot,
                      Reverse
                     )) of
        {_RawRows, _Count, _HasMore} = R ->
            {R, St0}
    catch
        error:{erlfdb_error, Code} ->
            ok = erlfdb:wait(erlfdb:on_error(Tx, Code)),
            St = iter_transaction_(St0),
            iter_future_(St)
    end.

%% @private
hit_data_limit_(_DataCount, 0) ->
    false;
hit_data_limit_(DataCount, IterLimit) ->
    DataCount + 1 > IterLimit.

%% @private
rows_more_last_(0, _DataCount, RawRows, _Count, HasMore0) ->
    {LastKey0, _} = lists:last(RawRows),
    {RawRows, HasMore0, LastKey0};
rows_more_last_(DataLimit, DataCount, RawRows, Count, HasMore0)
  when (DataLimit - DataCount) > Count ->
    {LastKey0, _} = lists:last(RawRows),
    {RawRows, HasMore0, LastKey0};
rows_more_last_(DataLimit, DataCount, RawRows, _Count, HasMore0) ->
    Rows0 = lists:sublist(RawRows, DataLimit - DataCount),
    NHasMore = HasMore0 orelse length(RawRows) > (DataLimit - DataCount),
    {LastKey0, _} = lists:last(Rows0),
    {Rows0, NHasMore, LastKey0}.

%% @doc The do_import is based off of the file:consult/1 code which reads in terms from a file
%% @private
do_import_(Table, SourceFile, ImportId) ->
    #st{record_name = RecName, fields = Fields, index = Index} = St = mfdb_manager:st(Table),
    IndexList = tl(tuple_to_list(Index)),
    ExpectLength = length(Fields) + 1,
    TypeCheckFun = case lists:all(fun(F) -> is_atom(F) end, Fields) of
                       true ->
                           fun(_,_) -> true end;
                       false ->
                           fun mfdb_lib:check_field_types/2
                   end,
    IndexCheckFun = case lists:all(fun(F) -> F =:= undefined end, tuple_to_list(Index)) of
                        true ->
                            fun(_,_) -> true end;
                        false ->
                            fun mfdb_lib:check_index_sizes/2
                    end,
    ImportFun =
        fun(ObjectTuple, Cnt) ->
                [RName | ObjectList] = tuple_to_list(ObjectTuple),
                [RKey | _] = ObjectList,
                InRec = {RName, size(ObjectTuple)},
                Expect = {RecName, ExpectLength},
                %% Functions must return 'true' to continue, anything else will exit early
                Flow = [{fun(X,Y) -> X =:= Y orelse {error, invalid_record} end, [InRec, Expect]},
                        {TypeCheckFun, [ObjectList, Fields]},
                        {IndexCheckFun, [ObjectList, IndexList]},
                        {fun mfdb_lib:put/3, [St, RKey, ObjectTuple]}],
                try mfdb_lib:flow(Flow, true) of
                    ok ->
                        Cnt + 1;
                    _ ->
                        Cnt
                catch
                    E:M:Stack ->
                        error_logger:error_msg("Import error: ~p", [{E,M,Stack}]),
                        Cnt
                end
        end,
    case file:open(SourceFile, [read]) of
        {ok, Fd} ->
            R = consult_stream(Fd, ImportFun, 0),
            _ = file:close(Fd),
            exit({normal, {ImportId, {ok, R}}});
        Error ->
            exit({normal, {ImportId, Error}})
    end.

%% @private
consult_stream(Fd, Line, Fun, Acc) ->
    case io:read(Fd, '', Line) of
        {ok, Term, EndLine} ->
            consult_stream(Fd, EndLine, Fun, Fun(Term, Acc));
        {error, Error, _Line} ->
            {error, Error};
        {eof, _Line} ->
            Acc
    end.

%% @private
consult_stream(Fd, Fun, Acc) ->
    _ = epp:set_encoding(Fd),
    consult_stream(Fd, 1, Fun, Acc).
