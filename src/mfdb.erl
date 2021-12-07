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
-export([update/3]).
-export([delete/2]).

-export([set_counter/3,
         update_counter/3,
         delete_counter/2]).

-export([lookup/2]).

-export([select/2,
         select/1]).

-export([index_read/3]).

-export([fold/3,
         fold/4]).

-export([subscribe/3,
         unsubscribe/2]).

-export([status/0]).

%% @private
-export([do_import_/3]).
-export([do_import_/4]).

%% gen_server API
-export([start_link/1,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([reaper_test_callback/1]).

-include("mfdb.hrl").
-define(REAP_POLL_INTERVAL, 5000).

-type dbrec() :: tuple().
-type foldfun() :: fun((fdb_tx(), dbrec(), any()) -> any()) | fun((dbrec(), any()) -> any()).

-export_type([dbrec/0, fdb_tx/0, foldfun/0]).

%% @doc Starts the management server for a table
-spec connect(Table :: table_name()) -> ok | {error, no_such_table}.
connect(Table) when is_atom(Table) ->
    gen_server:call(mfdb_manager, {connect, Table}).

%% @doc
%% Create a new table if it does not exist.
%% Options:
%%  - record: Record defines the record name and fields. Fields may be typed
%%  - indexes: Which fields, by position, to keep indexes for
%%  - table_ttl: Defines a TTL value for every record in the table. TTLs are set on insert
%%  - field_ttl: Uses a field of the record expiring records. The field must be typed as 'datetime'. Value is treated as a UTC timestamp
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
%% SourceFile is a file of erlang record terms of the correct type for the table.
%% @end
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
        InfoOpt =:= count orelse
        InfoOpt =:= fields orelse
        InfoOpt =:= indexes orelse
        InfoOpt =:= ttl orelse
        InfoOpt =:= ttl_callback) ->
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

%% @doc Lookup a record by Primary Key
-spec lookup(Table :: table_name(), PkValue :: any()) -> {ok, dbrec()} | {error, not_found}.
lookup(Table, PkValue) when is_atom(Table) ->
    #st{db = Db, pfx = TblPfx} = mfdb_manager:st(Table),
    EncKey = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, PkValue}),
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              case mfdb_lib:wait(erlfdb:get(Tx, EncKey)) of
                  not_found ->
                      {error, not_found};
                  EncVal ->
                      DecodedVal = mfdb_lib:decode_val(Tx, TblPfx, EncVal),
                      {ok, DecodedVal}
              end
      end).

%% @doc Look up records with specific index value and returns any matching objects
-spec index_read(Table :: table_name(), IdxValue :: any(), IdxPosition :: pos_integer()) -> [] | [dbrec()] | {error, no_index_on_field}.
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
            MatchSpec = [{list_to_tuple(MatchRec), [{'=:=', '$1', IdxValue}], ['$_']}],
            fold(Table, AccFun, [], MatchSpec)
    end.

%% @doc insert/replace a record. Types of values are checked if table has typed fields.
-spec insert(TableOrTx :: table_name() | fdb_tx(), DbRecord :: dbrec()) -> ok | {error, any()}.
insert(Table, DbRecord) when is_atom(Table) andalso is_tuple(DbRecord) ->
    #st{} = St = mfdb_manager:st(Table),
    insert(St, DbRecord);
insert(#st{record_name = RecName, fields = Fields, index = Index, ttl = Ttl} = St, DbRecord)
  when is_tuple(DbRecord) ->
    Flow = mk_insert_flow_(St, RecName, Fields, Index, Ttl, DbRecord),
    mfdb_lib:flow(Flow, true).

%% @private
mk_insert_flow_(StOrTx, RecName, Fields, Index, Ttl, ObjectTuple) ->
    TtlPos = case Ttl of
                 {field, Pos} -> Pos;
                 _ -> -1
             end,
    IndexList = tl(tuple_to_list(Index)),
    ExpectLength = length(Fields) + 1,
    TypeCheckFun = case lists:all(fun(F) -> is_atom(F) end, Fields) of
                       true ->
                           fun(_,_,_) -> true end;
                       false ->
                           fun mfdb_lib:check_field_types/3
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
    [{fun(X,Y) -> X =:= Y orelse {error, invalid_record} end, [InRec, Expect]},
     {TypeCheckFun, [ObjectList, Fields, TtlPos]},
     {IndexCheckFun, [ObjectList, IndexList]},
     {fun mfdb_lib:write/3, [StOrTx, RKey, ObjectTuple]}].

%% @doc Update an existing record
-spec update(TableOrTx :: table_name() | st(), Key :: any(), Changes :: field_changes()) -> ok | {error, not_found}.
update(Table, Key, Changes)
  when is_atom(Table) andalso
       is_list(Changes)  ->
    #st{} = St = mfdb_manager:st(Table),
    update(St, Key, Changes);
update(#st{record_name = RecordName, fields = Fields, index = Index, ttl = Ttl} = St, Key, Changes)
  when is_list(Changes) ->
    IndexList = tl(tuple_to_list(Index)),
    ChangeTuple = erlang:setelement(1, erlang:make_tuple(size(Index), ?NOOP_SENTINAL), RecordName),
    TtlPos = case Ttl of
                 {field, Pos} -> Pos;
                 _ -> -1
             end,
    case validate_updates_(Fields, Changes, 2, ChangeTuple, [], []) of
        {error, _} = Err ->
            Err;
        {UpdateRec, NVals, NFields} ->
            Flow = [{fun mfdb_lib:check_field_types/3, [NVals, NFields, TtlPos]},
                    {fun mfdb_lib:check_index_sizes/2, [NVals, IndexList]},
                    {fun mfdb_lib:update/3, [St, Key, UpdateRec]}],
            mfdb_lib:flow(Flow, true)
    end.

%% @private
%% Assign the values in changes to the correct positions in the ChangeTuple
%% and verify that the fields in the changes are defined for the record
validate_updates_([], [], _Pos, ChangeTuple, ValueAcc, TypeAcc) ->
    {ChangeTuple, ValueAcc, TypeAcc};
validate_updates_([], Changes, _Pos, _ChangeTuple, _ValueAcc, _TypeAcc) when Changes =/= [] ->
    {error, unknown_fields_in_update};
validate_updates_([{FieldName, FieldType} | Rest], Changes, Pos, ChangeTuple, ValueAcc, TypeAcc) ->
    case lists:keytake(FieldName, 1, Changes) of
        false ->
            %% Field not being changed, so set type to 'any'
            validate_updates_(Rest, Changes, Pos + 1, ChangeTuple, [?NOOP_SENTINAL | ValueAcc], [{FieldName, any} | TypeAcc]);
        {value, {FieldName, Value}, NChanges} ->
            NChangeTuple = erlang:setelement(Pos, ChangeTuple, Value),
            validate_updates_(Rest, NChanges, Pos + 1, NChangeTuple, [Value | ValueAcc], [{FieldName, FieldType} | TypeAcc])
    end.

%% @doc Atomic counter increment/decrement
-spec update_counter(Table :: table_name(), Key :: any(), Increment :: integer()) -> integer().
update_counter(Table, Key, Increment) when is_atom(Table) andalso is_integer(Increment) ->
    #st{db = Db, pfx = TabPfx} = mfdb_manager:st(Table),
    mfdb_lib:update_counter(Db, TabPfx, Key, Increment).

%% @doc Atomic counter delete
-spec delete_counter(Table :: table_name(), Key :: any()) -> ok.
delete_counter(Table, Key) when is_atom(Table) ->
    #st{db = Db, pfx = TabPfx} = mfdb_manager:st(Table),
    mfdb_lib:delete_counter(Db, TabPfx, Key).

%% @doc Atomic set of a counter value
-spec set_counter(Table :: table_name(), Key :: any(), Value :: integer()) -> ok.
set_counter(Table, Key, Value) when is_atom(Table) andalso is_integer(Value) ->
    #st{db = Db, pfx = TabPfx} = mfdb_manager:st(Table),
    mfdb_lib:set_counter(Db, TabPfx, Key, Value).

%%fold(Table, InnerFun, OuterAcc)
%%  when is_atom(Table) andalso
%%       (is_function(InnerFun, 2) orelse
%%        is_function(InnerFun, 3)) ->
%%    MatchSpec = [{'_',[],['$_']}],
%%    St0 = mfdb_manager:st(Table),
%%    St = case is_function(InnerFun, 3) of
%%             true -> St0#st{write_lock = true};
%%             false -> St0
%%         end,
%%    fold_cont_(select_(St, MatchSpec, 1, InnerFun, OuterAcc)).
%%fold(Table, InnerFun, OuterAcc, MatchSpec)
%%  when is_atom(Table) andalso
%%       (is_function(InnerFun, 2) orelse
%%        is_function(InnerFun, 3)) ->
%%    St0 = mfdb_manager:st(Table),
%%    St = case is_function(InnerFun, 3) of
%%             true -> St0#st{write_lock = true};
%%             false -> St0
%%         end,
%%    fold_cont_(select_(St, MatchSpec, 1, InnerFun, OuterAcc)).

%% @doc Delete a record from the table
-spec delete(TxOrTable :: table_name() | fdb_tx(), PkVal :: any()) ->  ok | {error, atom()}.
delete(Table, PkValue) when is_atom(Table) ->
    #st{} = St = mfdb_manager:st(Table),
    mfdb_lib:delete(St, PkValue);
delete(#st{} = Tx, PkValue) ->
    mfdb_lib:delete(Tx, PkValue).

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
    {Pid, Ref} = spawn_monitor(?MODULE, do_import_, [Table, SourceFile, ImportId]),
    OldWaiters = maps:get(waiters, State, []),
    NewWaiters = [{ImportId, Pid, Ref, From} | OldWaiters],
    {noreply, State#{waiters => NewWaiters}};
handle_call({table_info, all}, _From, #{table := Table} = State) ->
    #st{fields = Fields, index = Index, ttl = Ttl0, ttl_callback = TtlCb} = St = mfdb_manager:st(Table),
    Count = mfdb_lib:table_count(St),
    Size = mfdb_lib:table_data_size(St),
    Indexes = [P || #idx{pos = P} <- tuple_to_list(Index)],
    All0 = [{count, Count},
            {size, Size},
            {fields, Fields},
            {indexes, Indexes}],
    All1 = case TtlCb of
               undefined ->
                   All0;
               TtlCb ->
                   [{ttl_callback, TtlCb} | All0]
           end,
    All = case Ttl0 of
              {field, Pos} ->
                  [{field_ttl, Pos} | All1];
              {table, T} ->
                  [{table_ttl, T} | All1];
              undefined ->
                  All1
          end,
    {reply, {ok, All}, State};
handle_call({table_info, count}, _From, #{table := Table} = State) ->
    #st{} = St = mfdb_manager:st(Table),
    Count = mfdb_lib:table_count(St),
    {reply, {ok, Count}, State};
handle_call({table_info, size}, _From, #{table := Table} = State) ->
    #st{} = St = mfdb_manager:st(Table),
    Size = mfdb_lib:table_data_size(St),
    {reply, {ok, Size}, State};
handle_call({table_info, fields}, _From, #{table := Table} = State) ->
    #st{fields = Fields} = mfdb_manager:st(Table),
    {reply, {ok, Fields}, State};
handle_call({table_info, indexes}, _From, #{table := Table} = State) ->
    #st{index = Index} = mfdb_manager:st(Table),
    Indexes = [P || #idx{pos = P} <- tuple_to_list(Index)],
    {reply, {ok, Indexes}, State};
handle_call({table_info, ttl}, _From, #{table := Table} = State) ->
    #st{ttl = Ttl0} = mfdb_manager:st(Table),
    Ttl = case Ttl0 of
              {field, Pos} ->
                  {field_ttl, Pos};
              {table, T} ->
                  {table_ttl, T};
              undefined ->
                  no_ttl
          end,
    {reply, {ok, Ttl}, State};
handle_call({table_info, ttl_callback}, _From, #{table := Table} = State) ->
    #st{ttl_callback = TtlCb} = mfdb_manager:st(Table),
    {reply, {ok, TtlCb}, State};
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
    case Ttl of
        undefined ->
            %% No expiration
            ok;
        _ ->
            Now = erlang:universaltime(),
            RangeStart = mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, ?FDB_WC, ?FDB_WC}),
            RangeEnd = erlfdb_key:strinc(mfdb_lib:encode_prefix(TabPfx, {?TTL_TO_KEY_PFX, Now, ?FDB_END})),
            reap_expired_(St, RangeStart, RangeEnd, Now)
    end.

%% @private
reap_expired_(#st{db = Db, tab = Tab0, pfx = TabPfx0, ttl = {field, _FieldIdx}, ttl_callback = {Mod, Fun}}, RangeStart, RangeEnd, Now) ->
    %% Expiration with callbacks *only* supported for field-based TTLs,
    %% *NOT* Table based TTLs
    Tab = binary_to_existing_atom(Tab0),
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              KVs = mfdb_lib:wait(erlfdb:get_range(Tx, RangeStart, RangeEnd, [{limit, 100}])),
              LastKey =
                  lists:foldl(
                    fun({EncKey, <<>>}, LKey0) ->
                            %% Delete the actual expired record
                            <<PfxBytes:8, TabPfx/binary>> = TabPfx0,
                            <<PfxBytes:8, TabPfx:PfxBytes/binary, EncValue/binary>> = EncKey,
                            case sext:decode(EncValue) of
                                {?TTL_TO_KEY_PFX, {{_, _, _}, {_, _, _}} = Expires, RecKey} when Expires < Now ->
                                    %% get the record
                                    {ok, Record} = lookup(Tab, RecKey),
                                    %% Key2Ttl have to be removed individually.
                                    %% This must be done before handing to callback
                                    %% since callback may save record with a new TTL
                                    TtlK2T = mfdb_lib:encode_key(TabPfx, {?KEY_TO_TTL_PFX, RecKey}),
                                    ok = mfdb_lib:wait(erlfdb:clear(Tx, TtlK2T)),
                                    %% Pass to the callback
                                    try Mod:Fun(Record) of
                                        ok ->
                                            EncKey
                                    catch
                                        _E:_M:_Stack ->
                                            io:format("PROBLEM!!: ~p~n", [{_E,_M,_Stack}]),
                                            LKey0
                                    end;
                                _ ->
                                    LKey0
                            end
                    end, ok, KVs),
              case LastKey of
                  ok ->
                      ok;
                  LastKey ->
                      %% Finally clear the TTL2Rec entries for the covered range
                      mfdb_lib:wait(erlfdb:clear_range(Tx, RangeStart, erlfdb_key:strinc(LastKey)))
              end
      end);
reap_expired_(#st{db = Db, pfx = TabPfx0} = St, RangeStart, RangeEnd, Now) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              KVs = mfdb_lib:wait(erlfdb:get_range(Tx, RangeStart, RangeEnd, [{limit, 100}])),
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
              end
      end).

reaper_test_callback(Record) ->
    io:format("REAPER TEST: ~p~n", [Record]),
    ok.

%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%% INTERNAL %%%%%%%%%%%%%%%%%%%%%%%

%% @private
select_(Table, Matchspec0, Limit) when is_atom(Table) ->
    select_(mfdb_manager:st(Table), Matchspec0, Limit);
select_(#st{index = Indexes0} = St, Matchspec0, Limit) ->
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
            do_indexed_select(St, Ms, IdxParams, Limit);
        no_index ->
            do_select(St, Ms, PkStart, PkEnd, Limit)
    end.

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
             (Ms, {Inc, U, Acc}) when is_tuple(Ms) ->
                  Used = lists:foldl(
                           fun(M, UAcc) ->
                                   case is_atom(M) andalso atom_to_binary(M, utf8) of
                                       <<"\$", _/binary>> ->
                                           [M | UAcc];
                                       _ ->
                                           UAcc
                                   end
                           end, U, tuple_to_list(Ms)),
                  {Inc + 1, Used, Acc};
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
                            range_guards(Rest, Binds, Indexes, [{Idx, Comp, guard_val(Val)} | Acc]);
                        false ->
                            %% Column not indexed
                            range_guards(Rest, Binds, Indexes, Acc);
                        #idx{} ->
                            range_guards(Rest, Binds, Indexes, [{Idx, Comp, guard_val(Val)} | Acc])
                    end
            end;
        false ->
            range_guards(Rest, Binds, Indexes, Acc)
    end.

%% When tuples are used in the matchspec guards each tuple is wrapped in a tuple
%% There's probably a better way to handle the unwrapping
guard_val({ { { {Y, Mn, D} }, { { H, Mi, S} } } })
  when is_integer(Y) andalso
       is_integer(Mn) andalso
       is_integer(D) andalso
       is_integer(H) andalso
       is_integer(Mi) andalso
       is_number(S) ->
    %% curly-escaped Datetime
    {{Y, Mn, D}, { H, Mi, S}};
guard_val({ {W, X, Y, Z} })
  when is_integer(W) andalso
       is_integer(X) andalso
       is_integer(Y) andalso
       is_integer(Z) ->
    %% curly-escaped erlang ipv4
    {W, X, Y, Z};
guard_val({ {X, Y, Z} })
  when is_integer(X) andalso
       is_integer(Y) andalso
       is_integer(Z) ->
    %% curly-escaped erlang timestamp
    {X, Y, Z};
guard_val(Other) ->
    Other.

%% @private
primary_table_range_(Guards) ->
    primary_table_range_(Guards, undefined, undefined).

%% @private
primary_table_range_([], Start, End) ->
    {replace_(Start, ?FDB_WC), replace_(End, ?FDB_END)};
primary_table_range_([{2, '=:=', V} | Rest], undefined, _End) ->
    primary_table_range_(Rest, V, {lte, V});
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
            case mfdb_lib:wait(erlfdb:get(Tx, K)) of
                not_found ->
                    %% This should only happen with a dead index
                    %% entry (data deleted after we got index)
                    Acc0;
                V ->
                    Rec = mfdb_lib:wait(mfdb_lib:decode_val(Tx, TabPfx, V)),
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
do_select(#st{pfx = TabPfx} = St, MS, PkStart, PkEnd, Limit) ->
    KeysOnly = needs_key_only(MS),
    RangeStart = pk_to_range(TabPfx, start, PkStart),
    RangeEnd = pk_to_range(TabPfx, 'end', PkEnd),
    Keypat = {range, RangeStart, RangeEnd},
    CompiledMs = ets:match_spec_compile(MS),
    Iter = iter_(St, Keypat, KeysOnly, CompiledMs, undefined, [], Limit),
    do_iter(Iter, Limit, []).

%% @private
do_indexed_select(#st{pfx = TabPfx} = St, MS, {_IdxPos, {Start, End, Match, Guard}}, Limit) ->
    KeyMs = [{Match, Guard, ['$_']}],
    OKeysOnly = needs_key_only(MS),
    ICompiledKeyMs = ets:match_spec_compile(KeyMs),
    OCompiledKeyMs = ets:match_spec_compile(MS),
    OuterMatchFun = outer_match_fun_(TabPfx, OCompiledKeyMs),
    DataFun = index_match_fun_(ICompiledKeyMs, OuterMatchFun),
    IRange = {range, Start, End},
    Iter = iter_(St, IRange, OKeysOnly, undefined, DataFun, [], Limit),
    do_iter(Iter, Limit, []).

%% @private
do_iter('$end_of_table', _Limit, Acc) ->
    {mfdb_lib:sort(Acc), '$end_of_table'};
do_iter({Data, '$end_of_table'}, _Limit, Acc)
  when is_list(Data) andalso
       is_list(Acc) ->
    {mfdb_lib:sort(lists:append(Acc, Data)), '$end_of_table'};
do_iter({Data, '$end_of_table'}, _Limit, _Acc) ->
    {Data, '$end_of_table'};
do_iter({[], Iter}, Limit, Acc)
  when is_function(Iter, 0) ->
    %% io:format("case loop~n"),
    do_iter(Iter(), Limit, Acc);
do_iter({Data, Iter}, _Limit, Acc)
  when is_function(Iter) andalso
       is_list(Data) andalso
       is_list(Acc) ->
    %% io:format("case 0~n"),
    NAcc = mfdb_lib:sort(lists:append(Acc, Data)),
    {NAcc, Iter};
do_iter({Data, Iter}, _Limit, _Acc)
  when is_function(Iter) ->
    %% io:format("case 1~n"),
    {Data, Iter}.

%% @private
-spec iter_(St :: st(), StartKey :: any(), KeysOnly :: boolean(), Ms :: undefined | ets:comp_match_spec(), DataFun :: undefined | function(), InAcc :: list(), DataLimit :: pos_integer()) ->
          {list(), '$end_of_table' | continuation()}.
iter_(#st{db = Db, pfx = TabPfx}, StartKey0, KeysOnly, Ms, DataFun, InAcc, DataLimit) ->
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
             limit = 10000, %% we use a fix limit of 1000 for the number of KVs to pull
             target_bytes = 0,
             streaming_mode = iterator,%% it's *not* stream_iterator bad type spec in erlfdb_nif
             iteration = 1,
             snapshot = false,
             reverse = Reverse
            },
    case iter_transaction_(St0) of
        #iter_st{} = ItrSt ->
            iter_int_({cont, ItrSt});
        {error, Error} ->
            {error, Error, InAcc}
    end.

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
            {NewDataAcc0, AddCount} = iter_append_(Rows, Tx, TabPfx, KeysOnly, Ms,
                                                   DataFun, 0, DataAcc),
            NewDataAcc = case is_list(NewDataAcc0) of true -> lists:reverse(NewDataAcc0); false -> NewDataAcc0 end,
            NewDataCount = DataCount + AddCount,
            HitLimit = hit_data_limit_(NewDataCount, DataLimit),
            Done = RawRows =:= []
                orelse HitLimit =:= true
                orelse (HitLimit =:= false andalso HasMore =:= false),
            case Done of
                true when HasMore =:= false ->
                    %% no more rows
                    ok = iter_commit_(Tx),
                    {NewDataAcc, '$end_of_table'};
                true when HasMore =:= true ->
                    %% there are more rows, return accumulated data and a continuation fun
                    NSt0 = St#iter_st{
                             start_sel = erlfdb_key:first_greater_than(LastKey),
                             iteration = Iteration + 1,
                             data_count = 0,
                             data_acc = []
                            },
                    NSt = iter_transaction_(NSt0),
                    {NewDataAcc, fun() -> iter_int_({cont, NSt}) end};
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
        {idx, Idx} when is_function(DataFun, 3) ->
            %% This is a matched index with a supplied fun
            NAcc = DataFun(Tx, Idx, Acc),
            iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
        {idx, Idx} ->
            %% This is a matched index without a supplied fun
            case Ms =/= undefined andalso
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
            %% This is an actual record, not an index
            Rec = mfdb_lib:decode_val(Tx, TabPfx, V),
            case Ms =/= undefined andalso ets:match_spec_run([Rec], Ms) of
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
                    %% Record matched specification, but not a index operation
                    NAcc = [Match | Acc],
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc);
                [Match] when is_function(DataFun, 3) ->
                    %% Record matched specification, is a fold operation, apply the supplied DataFun
                    NAcc = DataFun(Tx, Match, Acc),
                    iter_append_(Rest, Tx, TabPfx, KeysOnly, Ms, DataFun, AddCount + 1, NAcc)
            end
    end.

%% @private
iter_val_(true, Key, _Rec) ->
    Key;
iter_val_(false, _Key, Rec) ->
    Rec.

%% @private
iter_start_end_(TblPfx, {range, S0, E0}) ->
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
    {S, E}.

%% @private
iter_transaction_(#iter_st{db = Db, tx = Tx} = St) ->
    case iter_commit_(Tx) of
        ok ->
            NTx = erlfdb:create_transaction(Db),
            erlfdb_nif:transaction_set_option(NTx, disallow_writes, 1),
            St#iter_st{tx = NTx};
        Error ->
            Error
    end.

%% @private
iter_commit_(undefined) ->
    ok;
iter_commit_(?IS_TX = Tx) ->
    try ok = mfdb_lib:commit(Tx)
    catch
        _E:{erlfdb_error, ErrCode}:_Stacktrace ->
            ok = mfdb_lib:wait(erlfdb:on_error(Tx, ErrCode), 5000),
            ErrAtom = mfdb_lib:fdb_err(ErrCode),
            mfdb_lib:wait(erlfdb:cancel(Tx)),
            {error, ErrAtom};
        _E:_M:_Stack ->
            error_logger:error_msg("unknown error: ~p ~p ~p", [_E, _M, _Stack]),
            mfdb_lib:wait(erlfdb:cancel(Tx)),
            {error, unknown_error}
    end.

%% @private
iter_future_(#iter_st{tx = Tx, start_sel = StartKey,
                      end_sel = EndKey, limit = Limit,
                      target_bytes = TargetBytes, streaming_mode = StreamingMode,
                      iteration = Iteration, snapshot = Snapshot,
                      reverse = Reverse} = St0) ->
    try mfdb_lib:wait(erlfdb_nif:transaction_get_range(
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
            %% We retry for get operations
            ok = mfdb_lib:wait(erlfdb:on_error(Tx, Code), 5000),
            St = iter_transaction_(St0),
            iter_future_(St)
    end.

%% @private
hit_data_limit_(DataCount, IterLimit) ->
    DataCount + 1 > IterLimit.

%% @private
rows_more_last_(DataLimit, DataCount, RawRows, Count, HasMore0)
  when (DataLimit - DataCount) > Count ->
    {LastKey0, _} = lists:last(RawRows),
    {RawRows, HasMore0, LastKey0};
rows_more_last_(DataLimit, DataCount, RawRows, _Count, HasMore0) ->
    Rows0 = lists:sublist(RawRows, DataLimit - DataCount),
    NHasMore = HasMore0 orelse length(RawRows) > (DataLimit - DataCount),
    {LastKey0, _} = lists:last(Rows0),
    {Rows0, NHasMore, LastKey0}.

%% @doc The do_import is based off of file:consult/1 code which reads in terms from a file
%% @hidden
do_import_(Table, SourceFile, ImportId) ->
    do_import_(Table, SourceFile, ImportId, false).

do_import_(Table, SourceFile, ImportId, Overwrite) ->
    #st{record_name = RecName, pfx = TblPfx, fields = Fields, index = Index, ttl = Ttl} = St = mfdb_manager:st(Table),
    Fname = iolist_to_binary(filename:basename(SourceFile)),
    TtlPos = case Ttl of
                 {field, Pos} -> Pos;
                 _ -> -1
             end,
    IndexList = tl(tuple_to_list(Index)),
    ExpectLength = length(Fields) + 1,
    TypeCheckFun = case lists:all(fun(F) -> is_atom(F) end, Fields) of
                       true ->
                           fun(_,_,_) -> true end;
                       false ->
                           fun mfdb_lib:check_field_types/3
                   end,
    IndexCheckFun = case lists:all(fun(F) -> F =:= undefined end, tuple_to_list(Index)) of
                        true ->
                            fun(_,_) -> true end;
                        false ->
                            fun mfdb_lib:check_index_sizes/2
                    end,
    WriteFun = case Overwrite of
                   true ->
                       fun mfdb_lib:write/3;
                   false ->
                       fun(#st{db = Db} = InSt, InKey, InObject) ->
                               EncKey = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, element(2, InObject)}),
                               case erlfdb:get(Db, EncKey) of
                                   not_found ->
                                       mfdb_lib:write(InSt, InKey, InObject);
                                   _EncVal ->
                                       skipped
                               end
                       end
               end,
    ImportFun =
        fun(ReplyTo, ObjectTuple) ->
                [RName | ObjectList] = tuple_to_list(ObjectTuple),
                [RKey | _] = ObjectList,
                InRec = {RName, size(ObjectTuple)},
                Expect = {RecName, ExpectLength},
                %% Functions must return 'true' to continue, anything else will exit early
                Flow = [{fun(X,Y) -> X =:= Y orelse {error, invalid_record} end, [InRec, Expect]},
                        {TypeCheckFun, [ObjectList, Fields, TtlPos]},
                        {IndexCheckFun, [ObjectList, IndexList]},
                        {WriteFun, [St, RKey, ObjectTuple]}],
                try mfdb_lib:flow(Flow, true) of
                    ok ->
                        ReplyTo ! {ok, 1};
                    skipped ->
                        ReplyTo ! {ok, 0};
                    _Other ->
                        error_logger:error_msg("~s Import error: ~p ~p", [Fname, _Other, ObjectTuple]),
                        ReplyTo ! {ok, 0}
                catch
                    E:M:Stack ->
                        error_logger:error_msg("~s Import error: ~p", [Fname, {E,M,Stack}]),
                        ReplyTo ! {ok, 0}
                end
        end,
    case file:open(SourceFile, [read]) of
        {ok, Fd} ->
            R = consult_stream(Fd, Fname, ImportFun),
            _ = file:close(Fd),
            exit({normal, {ImportId, {ok, R}}});
        Error ->
            exit({normal, {ImportId, Error}})
    end.

%% @private
consult_stream(Fd, Fname, Fun) ->
    _ = epp:set_encoding(Fd),
    consult_stream(Fd, 1, Fname, Fun, [], 0).

%% @private
consult_stream(Fd, Line, Fname, Fun, Acc0, Cnt) ->
    case io:read(Fd, '', Line) of
        {ok, Term, EndLine} ->
            NAcc = case (Cnt rem 5000) =:= 0 of
                       true ->
                           Self = self(),
                           Pids = [spawn(fun() -> Fun(Self, X) end) || X <- Acc0],
                           L = length(Pids),
                           rev_import_results(Fname, L, L, 0),
                           io:format("~s Import ~p so far~n", [Fname, Cnt]),
                           [Term];
                       false ->
                           [Term | Acc0]
                   end,
            consult_stream(Fd, EndLine, Fname, Fun, NAcc, Cnt + 1);
        {error, Error, _Line} ->
            {error, Error};
        {eof, _Line} ->
            Self = self(),
            Pids = [spawn(fun() -> Fun(Self, X) end) || X <- Acc0],
            L = length(Pids),
            rev_import_results(Fname, L, L, 0),
            ok
    end.

rev_import_results(Fname, Tot, 0, Added) ->
    io:format("~s Import added ~p of ~p completed~n", [Fname, Added, Tot]),
    ok;
rev_import_results(Fname, Tot, Cnt, Added) ->
    receive
        {ok, Add} ->
            rev_import_results(Fname, Tot, Cnt - 1, Added + Add)
    after
        60000 ->
            error_logger:error_msg("~s Import failed", [Fname]),
            ok
    end.

%%%  New fold impl %%%%

%% @doc Applies a function to all records in the table
%% InnerFun is either a 2-arity (read-only) or 3-arity (grabs read/write locks) function with specification:
%%   Fun(Record :: dbrec(), Accumulator :: any())
%%     - the 2-arity fold is read-only
%%   Fun(Tx :: opaque(), Record :: dbrec(), Accumulator :: any())
%%     - with the 3-arity function read/write locks are taken over each step
%%     - Tx is the internal transaction and is used instead of DbName in any calls to insert/2 or delete/2 inside the Fun
%%  Since InnerFun is applied _before_ the transaction is committed you
%%  should not perform external operations using the records within the InnerFun
%%  A contrived example:
%%    InnerFun = fun(Tx, #test{id = Id} = R, Deleted) ->
%%                   case Id > 5 andalso Id < 10 of
%%                       true ->
%%                           ok = mfdb:delete(Tx, Id),
%%                           [Id | Deleted];
%%                       false ->
%%                           Deleted
%%                   end
%%                end,
%%    AfterRecs = case mfdb:fold(test, InnerFun, []) of
%%                    {ok, Recs0} -> Recs0;
%%                    {error, Err, Recs0} -> Recs0
%%                end,
%%    %% Now do external operations...
%%    lists:foreach(
%%        fun(Rec) ->
%%          case my_external_op(Rec) of
%%            ok ->
%%              ok;
%%            error ->
%%              %% safely write back to db
%%              mfdb:insert(test, Rec)
%%          end
%%        end, AfterRecs).
%% @end
-spec fold(Table :: table_name(), InnerFun :: foldfun(), OuterAcc :: any()) -> any().
fold(Table, InnerFun, InnerAcc)
  when is_atom(Table) andalso
       (is_function(InnerFun, 3) orelse
        is_function(InnerFun, 2)) ->
    MatchSpec = [{'_',[],['$_']}],
    St0 = mfdb_manager:st(Table),
    St = case is_function(InnerFun, 3) of
             true -> St0#st{write_lock = true};
             false -> St0
         end,
    ffold_type_(St, InnerFun, InnerAcc, MatchSpec).

%% @doc Applies a function to all records in the table matching the MatchSpec
%% InnerFun is either a 2-arity (read-only) or 3-arity (grabs read/write locks) function with specification:
%%   Fun(Record :: dbrec(), Accumulator :: any())
%%     - the 2-arity fold is read-only
%%   Fun(Tx :: opaque(), Record :: dbrec(), Accumulator :: any())
%%     - with the 3-arity function read/write locks are taken over each step
%%     - Tx is the internal transaction and is used instead of DbName in any calls to insert/2 or delete/2 inside the Fun
%%  A fold can be exited early by returning {'$exit_fold', Acc} from the foldfun
%%  Since InnerFun is applied _before_ the transaction is committed you
%%  should not perform external operations using the records within the InnerFun
%%  A contrived example:
%%    InnerFun = fun(Tx, #test{id = Id} = R, Deleted) ->
%%                   ok = mfdb:delete(Tx, Id),
%%                   [Id | Deleted]
%%                end,
%%    Ms = [{#test{id = '$1', _ = '_'}, [{'>', '$1', 5}, {'<', '$1', 10}], ['$_']}],
%%    AfterRecs = case mfdb:fold(test, InnerFun, [], Ms) of
%%                    {ok, Recs0} -> Recs0;
%%                    {error, Err, Recs0} -> Recs0
%%                end,
%%    %% Now do external operations...
%%    lists:foreach(
%%        fun(Rec) ->
%%          case my_external_op(Rec) of
%%            ok ->
%%              ok;
%%            error ->
%%              %% safely write back to db
%%              mfdb:insert(test, Rec)
%%          end
%%        end, AfterRecs).
%% @end
-spec fold(Table :: table_name(), InnerFun :: foldfun(), OuterAcc :: any(), MatchSpec :: ets:match_spec()) -> any().
fold(Table, InnerFun, InnerAcc, MatchSpec) ->
    St0 = mfdb_manager:st(Table),
    St = case is_function(InnerFun, 3) of
             true -> St0#st{write_lock = true};
             false -> St0
         end,
    ffold_type_(St, InnerFun, InnerAcc, MatchSpec).

ffold_type_(#st{pfx = TabPfx, index = Index} = St, UserFun, UserAcc, MatchSpec0) ->
    %%CompiledMs = ets:match_spec_compile(MatchSpec),
    {Guards, Binds, Ms} =
        case MatchSpec0 of
            [{ {{_V, '$1'}} , G, _}] ->
                {G, [{1, ['$1']}], MatchSpec0};
            [{HP, G, MsRet}] ->
                {NewHP, NewGuards} = ms_rewrite(HP, G),
                NewMs = [{NewHP, NewGuards, MsRet}],
                {NewGuards, bound_in_headpat(NewHP), NewMs};
            _ ->
                {[], [], MatchSpec0}
        end,
    Indexes = [I || #idx{} = I <- tuple_to_list(Index)],
    %%io:format("Guards: ~p~n",[Guards]),
    RangeGuards = range_guards(Guards, Binds, Indexes, []),
    {PkStart0, PkEnd} = primary_table_range_(RangeGuards),
    RecMs = ets:match_spec_compile(Ms),
    case idx_sel(RangeGuards, Indexes, St) of
        {use_index, {_IdxPos, {Start, End, Match, Guard}}} = _IdxSel ->
            %%io:format("indexed~n"),
            IdxMs0 = [{Match, Guard, ['$_']}],
            IdxMs = ets:match_spec_compile(IdxMs0),
            RecMatchFun = ffold_rec_match_fun_(TabPfx, RecMs, UserFun),
            DataFun = ffold_idx_match_fun_(St, IdxMs, RecMatchFun),
            ffold_indexed_(St, DataFun, UserAcc, Start, End);
        no_index ->
            PkStart = case pk2pfx_(Ms) of
                          undefined ->
                              PkStart0;
                          PkPfx ->
                              %% io:format("Generated PkPrefix: ~p~nMs: ~p~n", [PkPfx,Ms]),
                              {pfx, PkPfx}
                      end,
            ffold_loop_init_(St, UserFun, UserAcc, RecMs, PkStart, PkEnd)
    end.

ffold_idx_match_fun_(#st{} = St, IdxMs, RecMatchFun) ->
    fun({idx, {{IdxVal, PkVal}} = R}, IAcc) ->
            case ets:match_spec_run([R], IdxMs) of
                [] ->
                    %% Did not match
                    IAcc;
                [{{IdxVal, PkVal}}] ->
                    %% Matched
                    RecMatchFun(St, PkVal, IAcc)
            end
    end.

ffold_rec_match_fun_(TabPfx, RecMs, UserFun) ->
    fun(#st{db = Db, write_lock = WLock} = St, PkVal, Acc0) ->
            EncKey = mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, PkVal}),
            Tx = erlfdb:create_transaction(Db),
            case mfdb_lib:wait(erlfdb:get(Tx, EncKey)) of
                not_found ->
                    %% This should only happen with a dead index
                    %% entry (data deleted after we got index)
                    mfdb_lib:wait(erlfdb:cancel(Tx)),
                    Acc0;
                EncVal0 ->
                    Rec0 = mfdb_lib:wait(mfdb_lib:decode_val(Tx, TabPfx, EncVal0)),
                    case ets:match_spec_run([Rec0], RecMs) of
                        [] ->
                            %% Did not match specification
                            ok = mfdb_lib:commit(Tx),
                            Acc0;
                        [Matched] when WLock =:= false ->
                            %% Matched specification
                            ok = mfdb_lib:commit(Tx),
                            UserFun(Matched, Acc0);
                        [Matched] when WLock =:= true ->
                            %% Matched specification
                            ok = mfdb_lib:wait(erlfdb:add_read_conflict_key(Tx, EncKey)),
                            ok = mfdb_lib:wait(erlfdb:add_write_conflict_key(Tx, EncKey)),
                            try
                                NewAcc = mfdb_lib:wait(UserFun(St#st{db = Tx}, Matched, Acc0)),
                                ok = mfdb_lib:commit(Tx),
                                %%io:format("ffold_rec_match_fun_~p ~p commited~n", [Tx, self()]),
                                NewAcc
                            catch
                                error:{erlfdb_error, Code} ->
                                    error_logger:error_msg("ffold_rec_match_fun_Fold error ~p ~p", [Tx, mfdb_lib:fdb_err(Code)]),
                                    mfdb_lib:wait(erlfdb:on_error(Tx, Code), 5000),
                                    ok = mfdb_lib:wait(erlfdb:cancel(Tx)),
                                    Acc0;
                                _E:_M:_Stack ->
                                    ok = mfdb_lib:wait(erlfdb:cancel(Tx)),
                                    Acc0
                            end
                    end
            end
    end.

ffold_limit_(#st{write_lock = true}) -> 1;
ffold_limit_(_) -> 100.

ffold_idx_apply_fun_([], _TabPfx, _MatchFun, Acc) ->
    Acc;
ffold_idx_apply_fun_([{K, _V} | Rest], TabPfx, MatchFun, Acc) ->
    case MatchFun(mfdb_lib:decode_key(TabPfx, K), Acc) of
        {'$exit_fold', NAcc} ->
            {'$exit_fold', NAcc};
        NAcc ->
            ffold_idx_apply_fun_(Rest, TabPfx, MatchFun, NAcc)
    end.

ffold_indexed_(#st{db = Db, pfx = TabPfx, write_lock = WLock} = St, MatchFun, InAcc, {_, Start}, {_, End}) ->
    case erlfdb:get_range(Db, Start, End, [{limit, ffold_limit_(St)}]) of
        [] ->
            [];
        [{LastKey, _}] when WLock =:= true ->
            case MatchFun(mfdb_lib:decode_key(TabPfx, LastKey), InAcc) of
                {'$exit_fold', NAcc} ->
                    NAcc;
                NAcc ->
                    ffold_indexed_cont_(#st{db = Db} = St, MatchFun, NAcc, LastKey, End)
            end;
        KeyVals when WLock =:= false ->
            case ffold_idx_apply_fun_(KeyVals, TabPfx, MatchFun, InAcc) of
                {'$exit_fold', NAcc} ->
                    NAcc;
                NAcc ->
                    {LastKey, _} = lists:last(KeyVals),
                    ffold_indexed_cont_(#st{db = Db} = St, MatchFun, NAcc, LastKey, End)
            end
    end.

ffold_indexed_cont_(#st{db = Db, pfx = TabPfx, write_lock = WLock} = St, MatchFun, InAcc, Start0, End) ->
    Start = erlfdb_key:first_greater_than(Start0),
    case erlfdb:get_range(Db, Start, End, [{limit, ffold_limit_(St) + 1}]) of
        [] ->
            InAcc;
        [{Start0, _}] ->
            InAcc;
        [{Start0, _}, {LastKey, _}] when WLock =:= true ->
            case MatchFun(mfdb_lib:decode_key(TabPfx, LastKey), InAcc) of
                {'$exit_fold', NAcc} ->
                    NAcc;
                NAcc ->
                    ffold_indexed_cont_(#st{db = Db} = St, MatchFun, NAcc, LastKey, End)
            end;
        [{LastKey, _}, _] when WLock =:= true ->
            case MatchFun(mfdb_lib:decode_key(TabPfx, LastKey), InAcc) of
                {'$exit_fold', NAcc} ->
                    NAcc;
                NAcc ->
                    ffold_indexed_cont_(#st{db = Db} = St, MatchFun, NAcc, LastKey, End)
            end;
        [{LastKey, _}] when WLock =:= true ->
            case MatchFun(mfdb_lib:decode_key(TabPfx, LastKey), InAcc) of
                {'$exit_fold', NAcc} ->
                    NAcc;
                NAcc ->
                    ffold_indexed_cont_(#st{db = Db} = St, MatchFun, NAcc, LastKey, End)
            end;
        KeyVals0 when WLock =:= false ->
            KeyVals = lists:keydelete(Start0, 1, KeyVals0),
            case ffold_idx_apply_fun_(KeyVals, TabPfx, MatchFun, InAcc) of
                {'$exit_fold', NAcc} ->
                    NAcc;
                NAcc ->
                    {LastKey, _} = lists:last(KeyVals),
                    ffold_indexed_cont_(#st{db = Db} = St, MatchFun, NAcc, LastKey, End)
            end
    end.

ffold_loop_init_(#st{db = Db} = St, InnerFun, InnerAcc, Ms, PkStart, PkEnd) ->
    case first_(St, PkStart) of
        '$end_of_table' ->
            InnerAcc;
        KeyValues ->
            {LastKey, _, _} = lists:last(KeyValues),
            ffold_loop_cont_(#st{db = Db} = St, InnerFun, InnerAcc, Ms, PkEnd, LastKey, KeyValues)
    end.

ffold_loop_cont_(#st{} = St, InnerFun, InnerAcc, Ms, PkEnd, LastKey, KeyVals) ->
    %%error_logger:info_msg("LastKey: ~p KeyVals: ~p", [LastKey, KeyVals]),
    case ffold_loop_recs(KeyVals, St, InnerFun, Ms, InnerAcc) of
        {'$exit_fold', NewAcc} when is_list(NewAcc) ->
            lists:reverse(NewAcc);
        {'$exit_fold', NewAcc} ->
            NewAcc;
        NewAcc ->
            case next_(St, LastKey, PkEnd) of
                '$end_of_table' when is_list(NewAcc) ->
                    lists:reverse(NewAcc);
                '$end_of_table' ->
                    NewAcc;
                [] when is_list(NewAcc) ->
                    lists:reverse(NewAcc);
                [] ->
                    NewAcc;
                KeyValues ->
                    {NewLastKey, _, _} = lists:last(KeyValues),
                    ffold_loop_cont_(St, InnerFun, NewAcc, Ms, PkEnd, NewLastKey, KeyValues)
            end
    end.

ffold_loop_recs([], _St, _InnerFun, _Ms, InnerAcc) ->
    InnerAcc;
ffold_loop_recs([{EncKey, _Key, Rec} | Rest], #st{write_lock = WLock} = St, InnerFun, Ms, InnerAcc) ->
    case ets:match_spec_run([Rec], Ms) of
        [] ->
            %% Record did not match specification
            ffold_loop_recs(Rest, St, InnerFun, Ms, InnerAcc);
        [_Match] when is_function(InnerFun, 3) andalso WLock =:= true ->
            case ffold_apply_with_transaction_(St, EncKey, InnerFun, Ms, InnerAcc) of
                {'$exit_fold', NewAcc} ->
                    {'$exit_fold', NewAcc};
                NewAcc ->
                    ffold_loop_recs(Rest, St, InnerFun, Ms, NewAcc)
            end;
        [Match] when is_function(InnerFun, 2) andalso WLock =:= false ->
            %% Record matched specification, is a fold operation, apply the supplied DataFun
            case InnerFun(Match, InnerAcc) of
                {'$exit_fold', NewAcc} ->
                    {'$exit_fold', NewAcc};
                NewAcc ->
                    ffold_loop_recs(Rest, St, InnerFun, Ms, NewAcc)
            end
    end.

%%ffold_apply_with_transaction_(#st{db = Db, pfx = TblPfx} = St, EncKey, InnerFun, Ms, InnerAcc) ->
%%    Tx = erlfdb:create_transaction(Db),
%%    ok = mfdb_lib:wait(erlfdb:add_read_conflict_key(Tx, EncKey)),
%%    ok = mfdb_lib:wait(erlfdb:add_write_conflict_key(Tx, EncKey)),
%%    case mfdb_lib:wait(erlfdb:get(Tx, EncKey)) of
%%        not_found ->
%%            %% move on.... Already deleted by something else
%%            ok = mfdb_lib:wait(erlfdb:cancel(Tx)),
%%            InnerAcc;
%%        EncVal ->
%%            Rec2 = mfdb_lib:wait(mfdb_lib:decode_val(Tx, TblPfx, EncVal)),
%%            case ets:match_spec_run([Rec2], Ms) of
%%                [Match] ->
%%                    try
%%                        NewAcc = mfdb_lib:wait(InnerFun(St#st{db = Tx}, Match, InnerAcc)),
%%                        ok = mfdb_lib:commit(Tx),
%%                        %%io:format("~p ~p commited~n", [Tx, self()]),
%%                        NewAcc
%%                    catch
%%                        error:{erlfdb_error, Code} ->
%%                            error_logger:error_msg("ffold_apply_with_transaction_ Fold error ~p ~p", [Tx, mfdb_lib:fdb_err(Code)]),
%%                            mfdb_lib:wait(erlfdb:on_error(Tx, Code), infinity),
%%                            ok = mfdb_lib:wait(erlfdb:cancel(Tx)),
%%                            InnerAcc;
%%                        _E:_M:_Stack ->
%%                            ok = mfdb_lib:wait(erlfdb:cancel(Tx)),
%%                            InnerAcc
%%                    end
%%            end
%%    end.

ffold_apply_with_transaction_(#st{db = Db, pfx = TblPfx} = St, EncKey, InnerFun, Ms, InnerAcc) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              ok = erlfdb:add_read_conflict_key(Tx, EncKey),
              ok = erlfdb:add_write_conflict_key(Tx, EncKey),
              case mfdb_lib:wait(erlfdb:get(Tx, EncKey)) of
                  not_found ->
                      %% move on.... Already deleted by something else
                      InnerAcc;
                  EncVal ->
                      Rec2 = mfdb_lib:wait(mfdb_lib:decode_val(Tx, TblPfx, EncVal)),
                      case ets:match_spec_run([Rec2], Ms) of
                          [Match] ->
                              try
                                  InnerFun(St#st{db = Tx}, Match, InnerAcc)
                              catch
                                  E:M:Stack ->
                                      error_logger:error_msg("Err applying InnerFun ~p", [{E,M,Stack}]),
                                      InnerAcc
                              end;
                          _ ->
                              InnerAcc
                      end
              end
      end).

range_start(TabPfx, {gt, X}) ->
    {mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X}), gt};
range_start(TabPfx, {gte, X}) ->
    {mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X}), gteq};
range_start(TabPfx, {pfx, X}) ->
    mfdb_lib:encode_prefix(TabPfx, {?DATA_PREFIX, X});
range_start(TabPfx, X) when X =:= '_' orelse X =:= <<"~">> ->
    mfdb_lib:encode_prefix(TabPfx, {?DATA_PREFIX, X});
range_start(TabPfx, X) ->
    mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X}).

range_end(TabPfx, {lt, X}) ->
    {mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X}), lteq};
range_end(TabPfx, {lte, X}) ->
    erlfdb_key:strinc(mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X}));
range_end(TabPfx, X) when X =:= '_' orelse X =:= <<"~">> ->
    erlfdb_key:strinc(mfdb_lib:encode_prefix(TabPfx, {?DATA_PREFIX, X}));
range_end(TabPfx, X) ->
    erlfdb_key:strinc(mfdb_lib:encode_key(TabPfx, {?DATA_PREFIX, X})).

first_(#st{db = Db, pfx = TabPfx} = St, PkStart) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              StartKey = range_start(TabPfx, PkStart),
              EndKey = erlfdb_key:strinc(mfdb_lib:encode_prefix(TabPfx, {?DATA_PREFIX, ?FDB_WC})),
              case mfdb_lib:wait(erlfdb:get_range(Tx, StartKey, EndKey, [{snapshot, false}, {limit, ffold_limit_(St)}])) of
                  [] ->
                      '$end_of_table';
                  KeyVals when is_list(KeyVals) ->
                      [{EncKey,
                        mfdb_lib:decode_key(TabPfx, EncKey),
                        mfdb_lib:wait(mfdb_lib:decode_val(Tx, TabPfx, Val))}
                       || {EncKey, Val} <- KeyVals];
                  _Other ->
                      error_logger:error_msg("unexpected: ~p", [_Other]),
                      '$end_of_table'
              end
      end).

next_(#st{db = Db, pfx = TabPfx} = St, PrevKey, PkEnd) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              SKey = range_start(TabPfx, mfdb_lib:decode_key(TabPfx, PrevKey)),
              EKey = range_end(TabPfx, PkEnd),
              try mfdb_lib:wait(erlfdb:get_range(Tx, SKey, EKey, [{snapshot, false}, {limit, ffold_limit_(St)+1}])) of
                  [] ->
                      '$end_of_table';
                  [{K, _V}] when K =:= PrevKey ->
                      '$end_of_table';
                  KeyVals0 ->
                      KeyVals = lists:keydelete(PrevKey, 1, KeyVals0),
                      [{EncKey,
                        mfdb_lib:decode_key(TabPfx, EncKey),
                        mfdb_lib:wait(mfdb_lib:decode_val(Tx, TabPfx, Val))}
                       || {EncKey, Val} <- KeyVals]
              catch
                  E:M:Stack ->
                      error_logger:error_msg("Error in next_/3: ~p", [{E,M,Stack}])
              end
      end).

%% Creates the longest possible prefix match for tuple-based pk field
pk2pfx_([{Rec, Guards0, _}])
  when is_tuple(Rec) andalso
       is_list(Guards0) ->
    Guards = lists:foldl(
               fun({'=:=', K, V}, Acc) ->
                       [{K, V} | Acc];
                  (_, Acc) ->
                       Acc
               end, [], Guards0),
    [_, Pk | _Tl] = tuple_to_list(Rec),
    case is_tuple(Pk) of
        true ->
            pk2pfx_mk_key_(tuple_to_list(Pk), Guards, []);
        false ->
            undefined
    end;
pk2pfx_(_) ->
    undefined.

pk2pfx_mk_key_([], _Guards, Key) ->
    list_to_tuple(lists:reverse(Key));
pk2pfx_mk_key_([?FDB_WC | _] = All, _Guards, Key) ->
    %% When we encounter a wildcard we need to change the rest of
    %% the matches to wildcards since we're generating a prefix match
    Tl = lists:duplicate(length(All), ?FDB_WC),
    list_to_tuple(lists:append(lists:reverse(Key), Tl));
pk2pfx_mk_key_([Val | Rest], Guards, Key) when is_atom(Val) ->
    case atom_to_binary(Val) of
        <<"\$", _/binary>> ->
            case lists:keyfind(Val, 1, Guards) of
                false ->
                    undefined;
                {Val, Match} ->
                    pk2pfx_mk_key_(Rest, Guards, [Match | Key])
            end;
        _Val ->
            pk2pfx_mk_key_(Rest, Guards, [Val | Key])
    end;
pk2pfx_mk_key_([Val | Rest], Guards, Key) ->
    pk2pfx_mk_key_(Rest, Guards, [Val | Key]).
