-module(mfdb_lib).

-compile({no_auto_import, [put/2, get/1]}).

-export([put/3,
         delete/2,
         bin_split/1,
         bin_join_parts/1,
         decode_key/2,
         decode_val/3,
         encode_key/2,
         encode_prefix/2,
         save_parts/4,
         idx_matches/3,
         idx_count_key/3,
         table_count/1,
         table_data_size/1,
         update_counter/3,
         validate_reply_/1]).

-export([clear_table/3]).

-export([flow/2,
         validate_record/1,
         validate_indexes/2,
         types_validation_fun/1]).

-include("mfdb.hrl").

-type flow_fun()    :: fun((...) -> any()).
-type flow_args()   :: list(any()).
-type flow()        :: {flow_fun(), flow_args()}.
-type flows()       :: list(flow()).

%% @doc
%% takes a list of {Fun, Args} and applies
%% each Fun(Args...) until last fun call or
%% non-match returned.
%% Final function can return anything
%% @end
-spec flow(flows(), term()) -> any().
flow(Flows, Match) ->
    flow_(Flows, Match, Match).

flow_([{Fun, Args} | Rest], Match, Match) when is_function(Fun) ->
    {arity, Arity} = erlang:fun_info(Fun, arity),
    case Arity =:= length(Args) of
        true ->
            flow_(Rest, Match, erlang:apply(Fun, Args));
        false ->
            {error, bad_arity}
    end;
flow_([{Fun, _Args} | _Rest], _Match, _Match) when not is_function(Fun) ->
    {error, invalid_flow};
flow_(_, _Match, Res) ->
    %% Non-matching or final result, so return
    Res.

%% @doc
%% insert or replace a record. If a record is unchanged it is not updated
%% in order to reduce the load potentially added by indexes
%% @end
put(#st{db = ?IS_DB = Db, pfx = TabPfx, hca_ref = HcaRef, index = Indexes}, PkValue, Record) ->
    %% Operation is on a data table
    EncKey = encode_key(TabPfx, {<<"dd">>, PkValue}),
    V1 = term_to_binary(Record),
    Size = byte_size(V1),
    %% Add size header as first 32-bits of value
    V = <<Size:32, V1/binary>>,
    Tx = erlfdb:create_transaction(Db),
    {DoSave, SizeInc, CountInc} =
        case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
            <<"mfdb_ref", OldSize:32, OldMfdbRefPartId/binary>> ->
                %% Remove any index values that may exist
                OldRecord = parts_value_(OldMfdbRefPartId, TabPfx, Tx),
                %% Has something in the record changed?
                RecordChanged = OldRecord =/= V1,
                case RecordChanged of
                    true ->
                        %% Remove any old index pointers
                        ok = remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(OldRecord), Indexes),
                        %% Replacing entry, increment by size diff
                        {<<"pt">>, PartHcaVal} = sext:decode(OldMfdbRefPartId),
                        Start = encode_prefix(TabPfx, {<<"pt">>, PartHcaVal, <<"_">>, '_'}),
                        ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
                        %% Replacing entry, only changes the size
                        {true, ((OldSize * -1) + Size), 0};
                    false ->
                        {false, 0, 0}
                end;
            <<OldSize:32, EncRecord/binary>> when EncRecord =/= V ->
                %% Record exists, but has changed
                %% Remove any old index pointers
                ok = remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(EncRecord), Indexes),
                %% Replacing entry, only changes the size
                {true, ((OldSize * -1) + Size), 0};
            <<_OldSize:32, EncRecord/binary>> when EncRecord =:= V ->
                %% Record already exists and is unchanged
                {false, 0, 0};
            not_found ->
                %% New entry, so increase count and add size
                {true, Size, 1}
        end,
    case DoSave of
        true ->
            case Size > ?MAX_VALUE_SIZE of
                true ->
                    %% Save the new parts
                    MfdbRefPartId = save_parts(Tx, TabPfx, HcaRef, V),
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", Size:32, MfdbRefPartId/binary>>));
                false ->
                    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, V))
            end,
            ok = create_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes),
            %% Update the 'size' of stored data
            ok = inc_counter_(Tx, tbl_size_key(TabPfx), SizeInc),
            %% Update the count of stored records
            ok = inc_counter_(Tx, tbl_count_key(TabPfx), CountInc),
            ok = erlfdb:wait(erlfdb:commit(Tx));
        false ->
            ok = erlfdb:wait(erlfdb:cancel(Tx))
    end,
    ok.

inc_counter_(_Tx, _Key, 0) ->
    ok;
inc_counter_(Tx, Key, Inc) ->
    erlfdb:wait(erlfdb:add(Tx, Key, Inc)).

create_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes) when is_tuple(Indexes) ->
    create_any_indexes_(Tx, TabPfx, PkValue, Record, tuple_to_list(Indexes));
create_any_indexes_(_Tx, _TabPfx, _PkValue, _Record, []) ->
    ok;
create_any_indexes_(Tx, TabPfx, PkValue, Record, [undefined | Rest]) ->
    create_any_indexes_(Tx, TabPfx, PkValue, Record, Rest);
create_any_indexes_(Tx, TabPfx, PkValue, Record, [#idx{pos = Pos, data_key = IdxDataKey, count_key = IdxCountKey} | Rest]) ->
    Value = element(Pos, Record),
    %% Increment the per-value counter for an index
    EncCountKey = idx_count_key(TabPfx, IdxCountKey, Value),
    inc_counter_(Tx, EncCountKey, 1),
    ok = erlfdb:wait(erlfdb:set(Tx, encode_key(TabPfx, {IdxDataKey, {Value, PkValue}}), <<>>)),
    create_any_indexes_(Tx, TabPfx, PkValue, Record, Rest).

parts_value_(MfdbRefPartId, TabPfx, Tx) ->
    {<<"pt">>, PartHcaVal} = sext:decode(MfdbRefPartId),
    Start = encode_prefix(TabPfx, {<<"pt">>, PartHcaVal, <<"_">>, '_'}),
    Parts = erlfdb:wait(erlfdb:get_range_startswith(Tx, Start)),
    bin_join_parts(Parts).

delete(#st{db = ?IS_DB = Db, pfx = TabPfx, index = Indexes}, PkValue) ->
    %% deleting a data item
    Tx = erlfdb:create_transaction(Db),
    EncKey = encode_key(TabPfx, {?DATA_PREFIX, PkValue}),
    {IncSize, IncCount} =
        case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
            not_found ->
                {0, 0};
            <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> ->
                %% Remove any index values that may exist
                EncRecord = parts_value_(MfdbRefPartId, TabPfx, Tx),
                ok = remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(EncRecord), Indexes),
                %% Remove parts of large value
                {<<"pt">>, PartHcaVal} = sext:decode(MfdbRefPartId),
                Start = encode_prefix(TabPfx, {<<"pt">>, PartHcaVal, <<"_">>, '_'}),
                ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
                {OldSize * -1, -1};
            <<OldSize:32, EncRecord/binary>> ->
                %% Remove any index values that may exist
                ok = remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(EncRecord), Indexes),
                {OldSize * -1, -1}
        end,
    %% decrement size
    inc_counter_(Tx, tbl_size_key(TabPfx), IncSize),
    %% decrement item count
    inc_counter_(Tx, tbl_count_key(TabPfx), IncCount),
    ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)),
    ok = erlfdb:wait(erlfdb:commit(Tx)).

remove_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes) when is_tuple(Indexes) ->
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, tuple_to_list(Indexes));
remove_any_indexes_(_Tx, _TabPfx, _PkValue, _Record, []) ->
    ok;
remove_any_indexes_(Tx, TabPfx, PkValue, Record, [undefined | Rest]) ->
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, Rest);
remove_any_indexes_(Tx, TabPfx, PkValue, Record, [#idx{pos = Pos, data_key = IdxDataKey, count_key = IdxCountKey} | Rest]) ->
    Value = element(Pos, Record),
    EncCountKey = idx_count_key(TabPfx, IdxCountKey, Value),
    inc_counter_(Tx, EncCountKey, -1),
    ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, encode_prefix(TabPfx, {IdxDataKey, {Value, PkValue}}))),
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, Rest).

idx_matches(#st{db = Db, index = Indexes, pfx = TabPfx}, IdxPos, Value) ->
    #idx{count_key = CountPfx} = element(IdxPos, Indexes),
    IdxCountKey = idx_count_key(TabPfx, CountPfx, Value),
    case erlfdb:get(Db, IdxCountKey) of
        not_found ->
            0;
        <<Count:64/unsigned-little-integer>> ->
            Count
    end.

table_data_size(#st{db = Db, pfx = TabPfx}) ->
    Key = tbl_size_key(TabPfx),
    case erlfdb:get(Db, Key) of
        not_found ->
            0;
        <<Count:64/unsigned-little-integer>> ->
            %% Convert byes to words
            erlang:round(Count / erlang:system_info(wordsize))
    end.

table_count(#st{db = Db, pfx = TabPfx}) ->
    Key = tbl_count_key(TabPfx),
    case erlfdb:get(Db, Key) of
        not_found ->
            0;
        <<Count:64/unsigned-little-integer>> ->
            Count
    end.

bin_split(Bin) ->
    bin_split(Bin, 0, []).

bin_split(<<>>, _, Acc) ->
    Acc;
bin_split(<<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>, Inc, Acc) ->
    bin_split(Rest, Inc + 1, [{Inc, Part} | Acc]);
bin_split(Tail, Inc, Acc) ->
    bin_split(<<>>, Inc, [{Inc, Tail} | Acc]).

-spec bin_join_parts(list({integer(), binary()})) -> binary().
bin_join_parts(BinList) ->
    bin_join_parts_(lists:keysort(1, BinList), <<>>).

bin_join_parts_([], Acc) ->
    Acc;
bin_join_parts_([{_, Bin} | Rest], Acc) ->
    bin_join_parts_(Rest, <<Acc/binary, Bin/binary>>).

decode_key(<<PfxBytes:8, TabPfx/binary>>,
           <<PfxBytes:8, TabPfx:PfxBytes/binary, EncValue/binary>>) ->
    case sext:decode(EncValue) of
        {<<"id", _/binary>>, {IdxVal, PkVal}} ->
            %% data index reference key
            %% so wrap in a tuple
            {idx, {{IdxVal, PkVal}}};
        {<<"dd">>, Key} ->
            Key;
        {<<"ic">>, Key} ->
            {cnt_idx, Key};
        {<<"pt">>, PartHcaVal} ->
            PartHcaVal;
        BadVal ->
            exit({TabPfx, BadVal})
    end.

decode_val(_Db, _TabPfx, <<>>) ->
    <<>>;
decode_val(Db, TabPfx, <<"mfdb_ref", _OldSize:32, MfdbRefPartId/binary>>) ->
    <<_:32, Val/binary>> = parts_value_(MfdbRefPartId, TabPfx, Db),
    binary_to_term(Val);
decode_val(_Db, _TabPfx, <<_:32, CodedVal/binary>>) ->
    binary_to_term(CodedVal).

encode_key(TabPfx, Key) ->
    <<TabPfx/binary, (sext:encode(Key))/binary>>.

encode_prefix(TabPfx, Key) ->
    <<TabPfx/binary, (sext:prefix(Key))/binary>>.

tbl_count_key(TabPfx) ->
    encode_key(TabPfx, {<<"tc">>}).

tbl_size_key(TabPfx) ->
    encode_key(TabPfx, {<<"ts">>}).

idx_count_key(TabPfx, IdxId, Value) ->
    encode_key(TabPfx, {IdxId, Value}).

save_parts(?IS_TX = Tx, TabPfx, Hca, Bin) ->
    PartId = erlfdb_hca:allocate(Hca, Tx),
    PartKey = sext:encode({<<"pt">>, PartId}),
    ok = save_parts_(Tx, TabPfx, PartId, 0, Bin),
    PartKey.

save_parts_(_Tx, _TabPfx, _PartId, _PartInc, <<>>) ->
    ok;
save_parts_(Tx, TabPfx, PartId, PartInc, <<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>) ->
    Key = encode_key(TabPfx, {<<"pt">>, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Part)),
    save_parts_(Tx, TabPfx, PartId, PartInc + 1, Rest);
save_parts_(Tx, TabPfx, PartId, PartInc, Tail) ->
    Key = encode_key(TabPfx, {<<"pt">>, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Tail)),
    save_parts_(Tx, TabPfx, PartId, PartInc + 1, <<>>).

update_counter(?IS_DB = Db, EncKey, Incr) ->
    %% Atomic counter increment
    %% Mnesia counters are dirty only, and cannot go below zero
    %%  dirty_update_counter({Tab, Key}, Incr) -> NewVal | exit({aborted, Reason})
    %%    Calls mnesia:dirty_update_counter(Tab, Key, Incr).
    %%
    %%  dirty_update_counter(Tab, Key, Incr) -> NewVal | exit({aborted, Reason})
    %%    Mnesia has no special counter records. However, records of
    %%    the form {Tab, Key, Integer} can be used as (possibly disc-resident)
    %%    counters when Tab is a set. This function updates a counter with a positive
    %%    or negative number. However, counters can never become less than zero.
    %%    There are two significant differences between this function and the action
    %%    of first reading the record, performing the arithmetics, and then writing the record:
    %%
    %%    It is much more efficient. mnesia:dirty_update_counter/3 is performed as an
    %%    atomic operation although it is not protected by a transaction.
    %%    If two processes perform mnesia:dirty_update_counter/3 simultaneously, both
    %%    updates take effect without the risk of losing one of the updates. The new
    %%    value NewVal of the counter is returned.
    %%
    %%    If Key do not exists, a new record is created with value Incr if it is larger
    %%    than 0, otherwise it is set to 0.
    %%
    %% This implementation tries to follow the same pattern.
    %% Increment the counter and then read the new value
    %%  - if the new value is < 0 (not allowed), abort
    %%  - if the new value is > 0, return the new value
    %% The exception:
    %%   if the counter does not exist and is created with a Incr < 0
    %%    we abort instead of creating a counter with value of '0'
    %% NOTE: FDB counters will wrap: EG Incr 0 by -1 wraps to max value
    Tx = erlfdb:create_transaction(Db),
    try do_update_counter(Tx, EncKey, Incr)
    catch
        error:{erlfdb_error,1025}:_Stack ->
            lager:info("Transaction cancelled, retrying"),
            erlfdb:wait(erlfdb:cancel(Tx)),
            update_counter(Db, EncKey, Incr)
    end.

do_update_counter(Tx, EncKey, Increment) ->
    Old = case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
              not_found ->
                  erlfdb:wait(erlfdb:add(Tx, EncKey, 0)),
                  0;
              <<OldVal:64/unsigned-little-integer>> ->
                  OldVal
          end,
    case (Old + Increment) of
        N when N < 0 ->
            %% Set counter to zero
            ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<0:64/unsigned-little-integer>>));
        _N ->
            %% Increment the counter
            %% This could very well not be what was expected
            %% since the counter may have been incremented
            %% by other transactions
            ok = erlfdb:wait(erlfdb:add(Tx, EncKey, Increment))
    end,
    %% Read the updated counter value
    <<NewVal:64/unsigned-little-integer>> = erlfdb:wait(erlfdb:get(Tx, EncKey)),
    erlfdb:wait(erlfdb:commit(Tx)),
    NewVal.

validate_indexes([], _Record) ->
    ok;
validate_indexes(Indexes, {_, Fields}) ->
    Allowed = lists:seq(3, length(Fields) + 1),
    case Indexes -- Allowed of
        [] ->
            Sorted = lists:sort(Indexes),
            Unique = lists:usort(Indexes),
            case Unique =:= Sorted of
                true ->
                    ok;
                false ->
                    {error, index_duplicate_position, Sorted -- Unique}
            end;
        Invalid ->
            {error, index_out_of_range, Invalid}
    end.

validate_record({RecName, Fields})
  when is_atom(RecName) andalso
       is_list(Fields) andalso
       Fields =/= [] ->
    validate_fields_(Fields);
validate_record(_) ->
    {error, invalid_record}.

validate_fields_([]) ->
    ok;
validate_fields_([Name | Rest])
  when is_atom(Name) ->
    %% Untyped field
    validate_fields_(Rest);
validate_fields_([{Name, Type} | Rest])
  when is_atom(Name) andalso is_atom(Type) ->
    case lists:member(Type, ?FIELD_TYPES) of
        true ->
            validate_fields_(Rest);
        false ->
            {error, invalid_record_field, Name}
    end;
validate_fields_([{Name, Types} | Rest])
  when is_atom(Name) andalso is_list(Types) ->
    case lists:all(fun(T) -> lists:member(T, ?FIELD_TYPES) end, Types) of
        true ->
            validate_fields_(Rest);
        false ->
            {error, invalid_record_field, Name}
    end;
validate_fields_([H | _]) ->
    {error, invalid_record_field, H}.

%% Ensure that no value on an indexed field exceeds the key size restriction
value_size_guard_([], _TblPfx, _Fields, _Obj) ->
    ok;
value_size_guard_([#idx{pos = P, data_key = IdxDataKey} | Rest], TblPfx, Fields, Obj) ->
    IdxVal = element(P, Obj),
    IdxId = element(2, Obj),
    Idx = {{IdxVal, IdxId}},
    EncKey = mfdb_lib:encode_key(TblPfx, {IdxDataKey, Idx}),
    ByteSize = byte_size(EncKey),
    case ByteSize of
        X when X > ?MAX_KEY_SIZE ->
            {error, field_name(lists:nth(P - 1, Fields))};
        _ ->
            value_size_guard_(Rest, TblPfx, Fields, Obj)
    end.

field_name(N) when is_atom(N) ->
    N;
field_name({N, _Type}) when is_atom(N) ->
    N.

types_validation_fun(#st{pfx = TblPfx, fields = Fields, index = Indexes0}) ->
    %% Using the field definition generate a Fun
    %% which validates the `type` of values provided
    %% before a record is inserted. This is strict typing,
    %% only supporting a single type per field
    %% IE it cannot support
    %% `undefined | null | binary()` type constructs
    Indexes = [Idx || #idx{} = Idx <- tuple_to_list(Indexes0)],
    fun(Rec) ->
            case value_size_guard_(Indexes, TblPfx, Fields, Rec) of
                ok ->
                    RecValues = tl(tuple_to_list(Rec)),
                    type_checks_(RecValues, Fields);
                Error ->
                    Error
            end
    end.

type_checks_([], []) ->
    true;
type_checks_([_Val | RVals], [Name | RTypes]) when is_atom(Name) ->
    type_checks_(RVals, RTypes);
type_checks_([_Val | RVals], [{_Name, any} | RTypes]) ->
    type_checks_(RVals, RTypes);
type_checks_([_Val | RVals], [{_Name, term} | RTypes]) ->
    type_checks_(RVals, RTypes);
type_checks_([Val | RVals], [{Name, Type} | RTypes]) when is_atom(Type) ->
    case type_check_(Val, Type) of
        true ->
            type_checks_(RVals, RTypes);
        false ->
            {error, invalid_type, Name}
    end;
type_checks_([Val | RVals], [{Name, Types} | RTypes]) when is_list(Types) ->
    case lists:any(fun(T) -> type_check_(Val, T) end, Types) of
        true ->
            type_checks_(RVals, RTypes);
        false ->
            {error, invalid_type, Name}
    end.

type_check_(undefined, undefined) ->
    true;
type_check_(null, null) ->
    true;
type_check_(InVal, binary) ->
    is_binary(InVal);
type_check_(InVal, integer) ->
    is_integer(InVal);
type_check_(InVal, float) ->
    is_float(InVal);
type_check_(InVal, list) ->
    is_list(InVal);
type_check_(InVal, tuple) ->
    is_tuple(InVal);
type_check_({_,_,_} = D, date) ->
    calendar:valid_date(D);
type_check_(_InVal, date) ->
    false;
type_check_({D, {H, M, S}}, datetime) ->
    (calendar:valid_date(D) andalso
     is_integer(H) andalso is_integer(M) andalso is_integer(S) andalso
     H >= 0 andalso H < 24 andalso
     M >= 0 andalso M < 60 andalso
     H < 24 andalso S >= 0 andalso S < 60);
type_check_(_InVal, datetime) ->
    false;
type_check_({H, M, S}, time) ->
    (is_integer(H) andalso is_integer(M) andalso is_integer(S) andalso
     H >= 0 andalso H < 24 andalso
     M >= 0 andalso M < 60 andalso
     H < 24 andalso S >= 0 andalso S < 60);
type_check_(_InVal, time) ->
    false;
type_check_(InVal, inet) ->
    try inet_parse:ntoa(InVal) of
        _ ->
            true
    catch
        _:_:_ ->
            false
    end;
type_check_({_,_,_,_} = InVal, inet4) ->
    try inet_parse:ntoa(InVal) of
        _ ->
            true
    catch
        _:_:_ ->
            false
    end;
type_check_(_InVal, inet4) ->
    false;
type_check_({_, _, _, _, _, _, _, _} = InVal, inet6) ->
    try inet_parse:ntoa(InVal) of
        _ ->
            true
    catch
        _:_:_ ->
            false
    end;
type_check_(_InVal, inet6) ->
    false;
type_check_(InVal, atom) ->
    is_atom(InVal);
type_check_(_InVal, _) ->
    false.

clear_table(Db, TblPfx, Indexes) ->
    ok = clear_indexes_(Db, TblPfx, Indexes),
    clear_data_(Db, TblPfx).

clear_data_(Db, TblPfx) ->
    %% Clear counters
    TblCountKey = tbl_count_key(TblPfx),
    ok = erlfdb:clear(Db, TblCountKey),
    TblSizeKey = tbl_size_key(TblPfx),
    ok = erlfdb:clear(Db, TblSizeKey),
    %% Clear 'parts'
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {<<"pt">>, ?FDB_WC, ?FDB_WC, ?FDB_WC})),
    %% Clear 'data'
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {<<"dd">>, ?FDB_WC})),
    %% Clear any other potential table-specific data
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {?FDB_WC, ?FDB_WC, ?FDB_WC})),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {?FDB_WC, ?FDB_WC})),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {?FDB_WC})).

clear_indexes_(Db, TblPfx, Indexes) when is_tuple(Indexes) ->
    clear_indexes_(Db, TblPfx, tuple_to_list(Indexes));
clear_indexes_(_Db, _TblPfx, []) ->
    ok;
clear_indexes_(Db, TblPfx, [undefined | Rest]) ->
    clear_indexes_(Db, TblPfx, Rest);
clear_indexes_(Db, TblPfx, [#idx{} = Idx | Rest]) ->
    ok = clear_index_(Db, TblPfx, Idx),
    clear_indexes_(Db, TblPfx, Rest).

clear_index_(Db, TblPfx, #idx{data_key = IdxDataKey, count_key = IdxCountKey}) ->
    %% Clear index data entries
    IdxDataStart = mfdb_lib:encode_key(TblPfx, {IdxDataKey, ?FDB_WC}),
    IdxDataEnd = mfdb_lib:encode_key(TblPfx, {IdxDataKey, ?FDB_END}),
    ok = erlfdb:clear_range(Db, IdxDataStart, IdxDataEnd),
    %% Clear per-value counters for an index
    IdxCountStart = mfdb_lib:encode_key(TblPfx, {IdxCountKey, ?FDB_WC}),
    IdxCountEnd = mfdb_lib:encode_key(TblPfx, {IdxCountKey, ?FDB_END}),
    ok = erlfdb:clear_range(Db, IdxCountStart, IdxCountEnd).

validate_reply_({notify, T}) when T =:= info orelse T =:= cast orelse T =:= call ->
    true;
validate_reply_({callback, Mod, Fun}) when is_atom(Mod) andalso is_atom(Fun) ->
    erlang:function_exported(Mod, Fun, 4);
validate_reply_(_) ->
    false.
