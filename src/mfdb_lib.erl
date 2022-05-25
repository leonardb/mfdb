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

-module(mfdb_lib).

-compile({no_auto_import, [put/2, get/1]}).

-export([write/3,
         delete/2,
         update/3,
         bin_split/1,
         bin_join_parts/1,
         decode_key/2,
         decode_val/3,
         encode_key/2,
         encode_prefix/2,
         save_parts/4,
         idx_matches/3,
         table_count/1,
         table_data_size/1,
         update_counter/5,
         delete_counter/3,
         read_counter/3,
         set_counter/5,
         validate_reply_/1,
         unixtime/0,
         expires/1,
         sort/1,
         sort/2,
         prefix_to_range/1,
         commit/1,
         wait/1,
         wait/2]).

-export([clear_table/3]).

-export([flow/2,
         validate_record/1,
         validate_indexes/2,
         check_field_types/3,
         check_index_sizes/2]).

-export([mk_tx/1,
         fdb_err/1]).

-include("mfdb.hrl").

-define(SECONDS_TO_EPOCH, (719528*24*3600)).
-define(ENTRIES_PER_COUNTER, 10).

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
    %% Here be dragons... You need to be careful when chaining the funs
    %% together and make sure all funs except final are restricted
    %% to Match or {err, any{}} type returns
    %% Non-matching or final result, so return
    Res.

sort([]) ->
    [];
sort([X | _] = L) when is_tuple(X) ->
    lists:keysort(2, L);
sort(L) ->
    lists:sort(L).

%% @doc
%% Inserts or replaces a record
%% @end
write(#st{db = ?IS_DB = Db, pfx = TabPfx, hca_ref = HcaRef, index = Indexes, ttl = Ttl}, PkValue, Record) ->
    erlfdb:transactional(Db, write_fun_(TabPfx, HcaRef, Indexes, Ttl, PkValue, Record));
write(#st{db = ?IS_TX = Tx, pfx = TabPfx, hca_ref = HcaRef, index = Indexes, ttl = Ttl}, PkValue, Record) ->
    erlfdb:transactional(Tx, write_fun_(TabPfx, HcaRef, Indexes, Ttl, PkValue, Record)).

write_fun_(TabPfx, HcaRef, Indexes, Ttl, PkValue, Record) ->
    TtlValue = ttl_val_(Ttl, Record),
    fun(Tx) ->
            EncKey = encode_key(TabPfx, {?DATA_PREFIX, PkValue}),
            EncRecord = term_to_binary(Record),
            Size = byte_size(EncRecord),
            {DoSave, SizeInc, CountInc} =
                case wait(erlfdb:get(Tx, EncKey)) of
                    <<"mfdb_ref", OldSize:32, OldMfdbRefPartId/binary>> ->
                        %% Remove any index values that may exist
                        OldEncRecord = parts_value_(OldMfdbRefPartId, TabPfx, Tx),
                        %% Has something in the record changed?
                        RecordChanged = OldEncRecord =/= EncRecord,
                        case RecordChanged of
                            true ->
                                %% Remove any old index pointers
                                ok = remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(OldEncRecord), Indexes),
                                %% Replacing entry, increment by size diff
                                {?DATA_PART_PREFIX, PartHcaVal} = sext:decode(OldMfdbRefPartId),
                                Start = encode_prefix(TabPfx, {?DATA_PART_PREFIX, PartHcaVal, <<"_">>, ?FDB_WC}),
                                ok = wait(erlfdb:clear_range_startswith(Tx, Start)),
                                %% Replacing entry, only changes the size
                                {true, ((OldSize * -1) + Size), 0};
                            false ->
                                {false, 0, 0}
                        end;
                    <<OldSize:32, OldEncRecord/binary>> when OldEncRecord =/= EncRecord ->
                        %% Record exists, but has changed
                        %% Remove any old index pointers
                        ok = wait(remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(OldEncRecord), Indexes)),
                        %% Replacing entry, only changes the size
                        {true, ((OldSize * -1) + Size), 0};
                    <<_OldSize:32, OldEncRecord/binary>> when OldEncRecord =:= EncRecord ->
                        %% Record already exists and is unchanged
                        {false, 0, 0};
                    not_found ->
                        %% New entry, so increase count and add size
                        {true, Size, 1}
                end,
            case DoSave of
                true ->
                    wait(write_(Tx, TabPfx, HcaRef, Size, EncKey, Size, EncRecord)),
                    ok = wait(create_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes)),
                    %% Update the 'size' of stored data
                    ok = wait(inc_counter_(Tx, TabPfx, ?TABLE_SIZE_PREFIX, SizeInc)),
                    %% Update the count of stored records
                    ok = wait(inc_counter_(Tx, TabPfx, ?TABLE_COUNT_PREFIX, CountInc)),
                    %% Create/update any TTLs
                    ok = wait(ttl_add(Tx, TabPfx, TtlValue, PkValue));
                false ->
                    wait(write_(Tx, TabPfx, HcaRef, Size, EncKey, Size, EncRecord)),
                    %% Update any TTLs
                    ok = wait(ttl_add(Tx, TabPfx, TtlValue, PkValue))
            end
    end.

write_(Tx, TabPfx, HcaRef, Size, EncKey, Size, EncRecord)
  when Size > ?MAX_VALUE_SIZE ->
    %% Record split into multiple parts
    MfdbRefPartId = save_parts(Tx, TabPfx, HcaRef, EncRecord),
    ok = wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", Size:32, MfdbRefPartId/binary>>));
write_(Tx, _TabPfx, _HcaRef, Size, EncKey, Size, EncRecord) ->
    ok = wait(erlfdb:set(Tx, EncKey, <<Size:32, EncRecord/binary>>)).

%% @doc
%% Update a record. This performs multiple read operations but with a write lock on the key.
%% @end
update(#st{db = ?IS_DB = Db} = St, PkValue, UpdateRec) ->
    Tx = erlfdb:create_transaction(Db),
    case update(St#st{db = Tx}, PkValue, UpdateRec) of
        {error, not_found} ->
            ok = wait(erlfdb:cancel(Tx)),
            {error, not_found};
        _ ->
            wait(erlfdb:commit(Tx))
    end;
update(#st{db = ?IS_TX = Tx, pfx = TblPfx} = St, PkValue, UpdateRec) ->
    EncKey = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, PkValue}),
    ok = wait(erlfdb:add_write_conflict_key(Tx, EncKey)),
    case wait(erlfdb:get(Tx, EncKey)) of
        not_found ->
            ok = wait(erlfdb:cancel(Tx)),
            {error, not_found};
        EncVal ->
            DecodedVal = mfdb_lib:decode_val(Tx, TblPfx, EncVal),
            Record = merge_record_(tuple_to_list(DecodedVal), tuple_to_list(UpdateRec)),
            ok = write(St, PkValue, Record)
    end.

merge_record_(Old, New) ->
    merge_record_(Old, New, []).

merge_record_([], [], Merged) ->
    list_to_tuple(lists:reverse(Merged));
merge_record_([OldValue | OldRest], [?NOOP_SENTINAL | NewRest], Merged) ->
    merge_record_(OldRest, NewRest, [OldValue | Merged]);
merge_record_([_OldValue | OldRest], [NewValue | NewRest], Merged) ->
    merge_record_(OldRest, NewRest, [NewValue | Merged]).

ttl_val_(Ttl, Record) ->
    case Ttl of
        {field, TtlField} ->
            element(TtlField, Record);
        {table, TtlVal} ->
            expires(TtlVal);
        undefined ->
            never
    end.

ttl_add(?IS_DB = Db, TabPfx, {{_,_,_}, {_,_,_}} = TtlDatetime, Key) ->
    Tx = erlfdb:create_transaction(Db),
    ok = ttl_add(Tx, TabPfx, TtlDatetime, Key),
    wait(erlfdb:commit(Tx));
ttl_add(?IS_TX = Tx, TabPfx, {{_,_,_}, {_,_,_}} = TtlDatetime0, Key) ->
    %% We need to be able to lookup in both directions
    %% since we use a range query for reaping expired records
    %% and we also need to remove the previous entry if a record gets updated
    TTLDatetime = ttl_minute(TtlDatetime0),
    ok = ttl_remove(Tx, TabPfx, TTLDatetime, Key),
    %% aligned to current minute + 1 minute
    wait(erlfdb:set(Tx, encode_key(TabPfx, {?TTL_TO_KEY_PFX, TTLDatetime, Key}), <<>>)),
    wait(erlfdb:set(Tx, encode_key(TabPfx, {?KEY_TO_TTL_PFX, Key}), sext:encode(TTLDatetime)));
ttl_add(_Tx, _TabPfx, _, _Key) ->
    %% Not a date-time, do not add ttl
    ok.

ttl_remove(_Tx, _TabPfx, undefined, _Key) ->
    ok;
ttl_remove(_Tx, _TabPfx, null, _Key) ->
    ok;
ttl_remove(_Tx, _TabPfx, never, _Key) ->
    ok;
ttl_remove(?IS_TX = Tx, TabPfx, _TtlDatetime, Key) ->
    TtlK2T = encode_key(TabPfx, {?KEY_TO_TTL_PFX, Key}),
    case wait(erlfdb:get(Tx, TtlK2T)) of
        not_found ->
            ok;
        TtlDatetime ->
            OldTtlT2K = {?TTL_TO_KEY_PFX, sext:decode(TtlDatetime), Key},
            wait(erlfdb:clear(Tx, encode_key(TabPfx, OldTtlT2K)))
    end,
    wait(erlfdb:clear(Tx, encode_key(TabPfx, {?KEY_TO_TTL_PFX, Key}))).

inc_counter_(_Tx, _TblPfx, _KeyPfx, 0) ->
    ok;
inc_counter_(Tx, TblPfx, Key0, Inc) ->
    %% Increment random counter
    Key1 = case Key0 of
               K0 when is_binary(K0) ->
                   {K0, rand:uniform(?ENTRIES_PER_COUNTER)};
               {K0, V0} ->
                   {K0, V0, rand:uniform(?ENTRIES_PER_COUNTER)}
           end,
    Key = encode_key(TblPfx, Key1),
    wait(erlfdb:add(Tx, Key, Inc)).

create_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes) when is_tuple(Indexes) ->
    create_any_indexes_(Tx, TabPfx, PkValue, Record, tuple_to_list(Indexes));
create_any_indexes_(_Tx, _TabPfx, _PkValue, _Record, []) ->
    ok;
create_any_indexes_(Tx, TabPfx, PkValue, Record, [undefined | Rest]) ->
    create_any_indexes_(Tx, TabPfx, PkValue, Record, Rest);
create_any_indexes_(Tx, TabPfx, PkValue, Record, [#idx{pos = Pos, data_key = IdxDataKey, count_key = IdxCountKey} | Rest]) ->
    Value = element(Pos, Record),
    %% Increment the per-value counter for an index
    inc_counter_(Tx, TabPfx, {IdxCountKey, Value}, 1),
    ok = wait(erlfdb:set(Tx, encode_key(TabPfx, {IdxDataKey, {Value, PkValue}}), <<>>)),
    create_any_indexes_(Tx, TabPfx, PkValue, Record, Rest).

parts_value_(MfdbRefPartId, TabPfx, Tx) ->
    {?DATA_PART_PREFIX, PartHcaVal} = sext:decode(MfdbRefPartId),
    Start = encode_prefix(TabPfx, {?DATA_PART_PREFIX, PartHcaVal, <<"_">>, ?FDB_WC}),
    Parts = wait(erlfdb:get_range_startswith(Tx, Start)),
    bin_join_parts(Parts).

mk_tx(#st{db = ?IS_DB = Db, write_lock = false} = St) ->
    FdbTx = erlfdb:create_transaction(Db),
    St#st{db = FdbTx};
mk_tx(#st{db = ?IS_DB = Db, write_lock = true} = St) ->
    FdbTx = erlfdb:create_transaction(Db),
    St#st{db = FdbTx}.

-spec delete(#st{}, any()) -> ok.
delete(#st{db = ?IS_DB} = OldSt, PkValue) ->
    %% deleting a data item
    #st{db = FdbTx} = NewSt = mk_tx(OldSt),
    case delete(NewSt, PkValue) of
        {error, not_found} ->
            ok;
        ok ->
            try wait(erlfdb:commit(FdbTx))
            catch
                error:{erlfdb_error, ErrCode} ->
                    ok = wait(erlfdb:on_error(FdbTx, ErrCode), 5000),
                    ErrAtom = mfdb_lib:fdb_err(ErrCode),
                    wait(erlfdb:cancel(FdbTx)),
                    {error, ErrAtom}
            end
    end;
delete(#st{db = ?IS_TX = Tx, pfx = TabPfx, index = Indexes}, PkValue) ->
    %% deleting a data item
    EncKey = encode_key(TabPfx, {?DATA_PREFIX, PkValue}),
    ok = wait(erlfdb:add_read_conflict_key(Tx, EncKey)),
    ok = wait(erlfdb:add_write_conflict_key(Tx, EncKey)),
    {IncSize, IncCount} =
        case wait(erlfdb:get(Tx, EncKey)) of
            not_found ->
                {0, 0};
            <<"mfdb_ref", OldSize:32, MfdbRefPartId/binary>> ->
                %% Remove any index values that may exist
                EncRecord = wait(parts_value_(MfdbRefPartId, TabPfx, Tx)),
                ok = wait(remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(EncRecord), Indexes)),
                %% Remove parts of large value
                {?DATA_PART_PREFIX, PartHcaVal} = sext:decode(MfdbRefPartId),
                Start = encode_prefix(TabPfx, {?DATA_PART_PREFIX, PartHcaVal, <<"_">>, ?FDB_WC}),
                ok = wait(erlfdb:clear_range_startswith(Tx, Start)),
                {OldSize * -1, -1};
            <<OldSize:32, EncRecord/binary>> ->
                %% Remove any index values that may exist
                ok = wait(remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(EncRecord), Indexes)),
                {OldSize * -1, -1}
        end,
    case {IncSize, IncCount} of
        {0, 0} ->
            {error, not_found};
        {IncSize, IncCount} ->
            %% decrement size
            wait(inc_counter_(Tx, TabPfx, ?TABLE_SIZE_PREFIX, IncSize)),
            %% decrement item count
            wait(inc_counter_(Tx, TabPfx, ?TABLE_COUNT_PREFIX, IncCount)),
            ok = wait(erlfdb:clear(Tx, EncKey))
    end.

remove_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes) when is_tuple(Indexes) ->
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, tuple_to_list(Indexes));
remove_any_indexes_(_Tx, _TabPfx, _PkValue, _Record, []) ->
    ok;
remove_any_indexes_(Tx, TabPfx, PkValue, Record, [undefined | Rest]) ->
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, Rest);
remove_any_indexes_(Tx, TabPfx, PkValue, Record, [#idx{pos = Pos, data_key = IdxDataKey, count_key = IdxCountKey} | Rest]) ->
    Value = element(Pos, Record),
    inc_counter_(Tx, TabPfx, {IdxCountKey, Value}, -1),
    ok = wait(erlfdb:clear_range_startswith(Tx, encode_prefix(TabPfx, {IdxDataKey, {Value, PkValue}}))),
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, Rest).

idx_matches(#st{db = Db, index = Indexes, pfx = TabPfx}, IdxPos, Value) ->
    #idx{count_key = CountPfx} = element(IdxPos, Indexes),
    Pfx = encode_prefix(TabPfx, {CountPfx, Value, ?FDB_WC}),
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              case wait(erlfdb:get_range_startswith(Tx, Pfx)) of
                  [] ->
                      0;
                  KVs ->
                      lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
              end
      end).

table_data_size(#st{db = Db, pfx = TabPfx}) ->
    Pfx = encode_prefix(TabPfx, {?TABLE_SIZE_PREFIX, ?FDB_WC}),
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              case erlfdb:get_range_startswith(Tx, Pfx) of
                  [] ->
                      0;
                  KVs ->
                      lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
              end
      end).

table_count(#st{db = ?IS_DB = Db, pfx = TabPfx}) ->
    Pfx = encode_prefix(TabPfx, {?TABLE_COUNT_PREFIX, ?FDB_WC}),
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              case wait(erlfdb:get_range_startswith(Tx, Pfx)) of
                  [] ->
                      0;
                  KVs ->
                      lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
              end
      end).

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
        {?DATA_PREFIX, Key} ->
            Key;
        {?IDX_COUNT_PREFIX, Key} ->
            {cnt_idx, Key};
        {?DATA_PART_PREFIX, PartHcaVal} ->
            PartHcaVal;
        {?TTL_TO_KEY_PFX, _, Key} ->
            Key;
        {?KEY_TO_TTL_PFX, Key} ->
            Key;
        BadVal ->
            exit({TabPfx, BadVal})
    end.

decode_val(_Tx, _TabPfx, <<>>) ->
    <<>>;
decode_val(Tx, TabPfx, <<"mfdb_ref", _OldSize:32, MfdbRefPartId/binary>>) ->
    <<_:32, Val/binary>> = FullVal = parts_value_(MfdbRefPartId, TabPfx, Tx),
    try
        binary_to_term(Val)
    catch
        error:badarg:_Stack ->
            try
                binary_to_term(FullVal)
            catch
                error:badarg:_Stack2 ->
                    error_logger:error_msg("decode failed: ~p", [FullVal]),
                    io:format("Bad bin: ~p~n", [FullVal]),
                    <<>>
            end
    end;
decode_val(_Tx, _TabPfx, <<_:32, CodedVal/binary>>) ->
    binary_to_term(CodedVal).

encode_key(TabPfx, Key) ->
    <<TabPfx/binary, (sext:encode(Key))/binary>>.

encode_prefix(TabPfx, Key) ->
    <<TabPfx/binary, (sext:prefix(Key))/binary>>.

save_parts(?IS_TX = Tx, TabPfx, Hca, Bin) ->
    PartId = wait(erlfdb_hca:allocate(Hca, Tx)),
    PartKey = sext:encode({?DATA_PART_PREFIX, PartId}),
    ok = save_parts_(Tx, TabPfx, PartId, 0, Bin),
    PartKey.

save_parts_(_Tx, _TabPfx, _PartId, _PartInc, <<>>) ->
    ok;
save_parts_(Tx, TabPfx, PartId, PartInc, <<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>) ->
    Key = encode_key(TabPfx, {?DATA_PART_PREFIX, PartId, <<"_">>, PartInc}),
    ok = wait(erlfdb:set(Tx, Key, Part)),
    save_parts_(Tx, TabPfx, PartId, PartInc + 1, Rest);
save_parts_(Tx, TabPfx, PartId, PartInc, Tail) ->
    Key = encode_key(TabPfx, {?DATA_PART_PREFIX, PartId, <<"_">>, PartInc}),
    ok = wait(erlfdb:set(Tx, Key, Tail)),
    save_parts_(Tx, TabPfx, PartId, PartInc + 1, <<>>).

update_counter(Db, TabPfx, Key, Shards, Incr) when Shards =:= undefined ->
    update_counter(Db, TabPfx, Key, ?ENTRIES_PER_COUNTER, Incr);
update_counter(?IS_DB = Db, TabPfx, Key, Shards, Incr) ->
    try do_update_counter(Db, TabPfx, Key, Shards, Incr),
        read_counter(Db, TabPfx, Key)
    catch
        error:{erlfdb_error,1020}:_Stack ->
            error_logger:error_msg("Update counter transaction cancelled, retrying"),
            update_counter(Db, TabPfx, Key, Shards, Incr);
        error:{erlfdb_error,1025}:_Stack ->
            error_logger:error_msg("Update counter transaction cancelled, retrying"),
            update_counter(Db, TabPfx, Key, Shards, Incr);
        Err:Msg:St ->
            error_logger:error_msg("Update counter error: ~p", [{Err, Msg, St}])
    end.

do_update_counter(Db, TabPfx, Key, Shards, Increment) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              %% Increment random counter
              EncKey = encode_key(TabPfx, {?COUNTER_PREFIX, Key, rand:uniform(Shards)}),
              wait(erlfdb:add(Tx, EncKey, Increment))
              %% Read the updated counter value
              %%read_counter(Tx, TabPfx, Key)
      end).

set_counter(?IS_DB = Db, TabPfx, Key, Shards, Value) when Shards =:= undefined ->
    set_counter(?IS_DB = Db, TabPfx, Key, ?ENTRIES_PER_COUNTER, Value);
set_counter(?IS_DB = Db, TabPfx, Key, Shards, Value) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              Pfx = encode_prefix(TabPfx, {?COUNTER_PREFIX, Key, ?FDB_WC}),
              EncKey = encode_key(TabPfx, {?COUNTER_PREFIX, Key, rand:uniform(Shards)}),
              ok = wait(erlfdb:clear_range_startswith(Tx, Pfx)),
              ok = wait(erlfdb:add(Tx, EncKey, Value))
      end).

read_counter(?IS_DB = Db, TabPfx, Key) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              Pfx = encode_prefix(TabPfx, {?COUNTER_PREFIX, Key, ?FDB_WC}),
              case erlfdb:get_range_startswith(Tx, Pfx) of
                  [] ->
                      0;
                  KVs ->
                      lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
              end
      end);
read_counter(?IS_TX = Tx, TabPfx, Key) ->
    Pfx = encode_prefix(TabPfx, {?COUNTER_PREFIX, Key, ?FDB_WC}),
    case wait(erlfdb:get_range_startswith(Tx, Pfx)) of
        [] ->
            0;
        KVs ->
            lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
    end.

delete_counter(?IS_DB = Db, TabPfx, Key) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              Pfx = encode_prefix(TabPfx, {?COUNTER_PREFIX, Key, ?FDB_WC}),
              ok = wait(erlfdb:clear_range_startswith(Tx, Pfx))
      end).

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
    %% Support multiple types for fields. eg [undefined, binary]
    case lists:all(fun(T) -> lists:member(T, ?FIELD_TYPES) end, Types) of
        true ->
            validate_fields_(Rest);
        false ->
            {error, invalid_record_field, Name}
    end;
validate_fields_([H | _]) ->
    {error, invalid_record_field, H}.

check_field_types(Values, Fields, TtlPos) ->
    %% count starts at 2 for fields
    check_field_types(Values, Fields, TtlPos, 2).

check_field_types([], [], _, _) ->
    true;
check_field_types([?NOOP_SENTINAL | RestVal], [_Field | RestFields], TtlPos, FPos) ->
    check_field_types(RestVal, RestFields, TtlPos, FPos + 1);
check_field_types([_Val | RestVal], [{_Field, Type} | RestFields], TtlPos, FPos)
  when Type =:= any orelse Type =:= term ->
    check_field_types(RestVal, RestFields, TtlPos, FPos + 1);
check_field_types([Val | RestVal], [{Field, datetime = Type} | RestFields], TtlPos, TtlPos) ->
    %% Special case to allow 'never' on a ttl datetime field
    case Val =:= never of
        true ->
            check_field_types(RestVal, RestFields, TtlPos, TtlPos + 1);
        false ->
            case type_check_(Val, Type) of
                true ->
                    check_field_types(RestVal, RestFields, TtlPos, TtlPos + 1);
                false ->
                    {error, {Field, Val, list_to_atom("not_a_" ++ atom_to_list(Type))}}
            end
    end;
check_field_types([Val | RestVal], [{Field, Type} | RestFields], TtlPos, FPos) ->
    case type_check_(Val, Type) of
        true ->
            check_field_types(RestVal, RestFields, TtlPos, FPos + 1);
        false when is_atom(Type) ->
            {error, {Field, Val, list_to_atom("not_" ++ atom_to_list(Type))}};
        false when is_list(Type) ->
            {error, {Field, Val, list_to_atom("not_" ++ string:join([atom_to_list(T) || T <- Type], "|"))}}
    end.

check_index_sizes([], []) ->
    true;
check_index_sizes([?NOOP_SENTINAL | RestVal], [_Idx | RestIdx]) ->
    check_index_sizes(RestVal, RestIdx);
check_index_sizes([_Val | RestVal], [undefined | RestIdx]) ->
    check_index_sizes(RestVal, RestIdx);
check_index_sizes([Val | RestVal], [#idx{pos = Pos} | RestIdx]) ->
    case byte_size(term_to_binary(Val)) of
        X when X > ?MAX_KEY_SIZE ->
            {error, {too_large_for_index, Pos}};
        _ ->
            check_index_sizes(RestVal, RestIdx)
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
type_check_(InVal, Types) when is_list(Types) ->
    %% Check that InVal is one of types in Type List
    lists:any(fun(T) -> type_check_(InVal, T) end, Types);
type_check_(_InVal, _) ->
    false.

clear_table(Db, TblPfx, Indexes) ->
    ok = clear_indexes_(Db, TblPfx, Indexes),
    ok = clear_ttls_(Db, TblPfx),
    clear_data_(Db, TblPfx).

clear_data_(Db, TblPfx) ->
    %% Clear counters
    ok = erlfdb:clear_range_startswith(Db, encode_prefix(TblPfx, {?TABLE_COUNT_PREFIX, ?FDB_WC})),
    ok = erlfdb:clear_range_startswith(Db, encode_prefix(TblPfx, {?TABLE_SIZE_PREFIX, ?FDB_WC})),
    %% Clear 'parts'
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {?DATA_PART_PREFIX, ?FDB_WC, ?FDB_WC, ?FDB_WC})),
    %% Clear 'data'
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {?DATA_PREFIX, ?FDB_WC})),
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

clear_ttls_(Db, TblPfx) ->
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {?TTL_TO_KEY_PFX, ?FDB_WC, ?FDB_WC})),
    ok = erlfdb:clear_range_startswith(Db, mfdb_lib:encode_prefix(TblPfx, {?KEY_TO_TTL_PFX, ?FDB_WC})).

validate_reply_({notify, T}) when T =:= info orelse T =:= cast orelse T =:= call ->
    true;
validate_reply_({callback, Mod, Fun}) when is_atom(Mod) andalso is_atom(Fun) ->
    erlang:function_exported(Mod, Fun, 4);
validate_reply_(_) ->
    false.

unixtime() ->
    datetime_to_unix(erlang:universaltime()).

ttl_minute({{_, _, _} = D, {H, M, _}}) ->
    %% Ensure 'at least' a minute
    unix_to_datetime(datetime_to_unix({D, {H, M, 0}}) + 60).

unix_to_datetime(Int) ->
    calendar:gregorian_seconds_to_datetime(?SECONDS_TO_EPOCH + Int).

datetime_to_unix({Mega, Secs, _}) ->
    (Mega * 1000000) + Secs;
datetime_to_unix({{Y,Mo,D},{H,Mi,S}}) ->
    calendar:datetime_to_gregorian_seconds(
      {{Y,Mo,D},{H,Mi,round(S)}}) - ?SECONDS_TO_EPOCH.

sort(_RecName, [] = L) ->
    L;
sort(RecName, [T | _] = L) when is_tuple(T) andalso element(1, T) =:= RecName ->
    lists:keysort(2, L);
sort(_RecName, L) ->
    lists:sort(L).

-spec expires(never | undefined | ttl_periods() | ttl_period()) -> never | calendar:datetime().
expires(undefined) ->
    %% never expires
    never;
expires(never) ->
    %% never expires
    never;
expires({unix, X}) ->
    {D, {H, M, _S}} = unix_to_datetime(X),
    {D, {H, M, 0}};
expires({_, _, _} = V) ->
    %% erlang timestamp
    {D, {H, M, _S}} = unix_to_datetime(datetime_to_unix(V)),
    {D, {H, M, 0}};
expires({{_, _, _} = D, {H, M, _}}) ->
    %% datetime
    {D, {H, M, 0}};
expires(E) when is_list(E) ->
    %% we're assuming a list of time intervals
    case lists:foldl(fun(_, {error, _} = Err) ->
                             Err;
                        (V, Acc) ->
                             case i2s(V) of
                                 {error, _} = Err ->
                                     Err;
                                 S ->
                                     S + Acc
                             end
                     end, 0, E) of
        {error, _} = Error ->
            Error;
        Secs ->
            {D, {H, M, _S}} = unix_to_datetime(unixtime() + Secs),
            {D, {H, M, 0}}
    end;
expires({Period, Inc} = P)
  when (Period =:= minutes orelse
        Period =:= hours orelse
        Period =:= days) andalso
       is_integer(Inc) ->
    case i2s(P) of
        X when is_integer(X) ->
            {D, {H, M, _S}} = unix_to_datetime(unixtime() + X),
            {D, {H, M, 0}};
        Err ->
            throw(Err)
    end;
expires(_Expires) ->
    try throw(invalid_expires)
    catch
        _:_:St ->
            error_logger:error_msg("Invalid expires: ~p ~p", [_Expires, St]),
            throw(invalid_expires)
    end.

i2s({minutes, M}) -> (M * 60);
i2s({hours, H}) -> (H * 60 * 60);
i2s({days, D}) -> (D * 86400);
i2s(_) -> {error, invalid_expires}.

prefix_to_range(Prefix) ->
    EndKey = erlfdb_key:strinc(Prefix),
    {Prefix, EndKey}.

fdb_err(0) -> success;
fdb_err(1000) -> operation_failed;
fdb_err(1004) -> timed_out;
fdb_err(1007) -> transaction_too_old;
fdb_err(1009) -> future_version;
fdb_err(1020) -> not_committed;
fdb_err(1021) -> commit_unknown_result;
fdb_err(1025) -> transaction_cancelled;
fdb_err(1031) -> transaction_timed_out;
fdb_err(1032) -> too_many_watches;
fdb_err(1034) -> watches_disabled;
fdb_err(1036) -> accessed_unreadable;
fdb_err(1037) -> process_behind;
fdb_err(1038) -> database_locked;
fdb_err(1039) -> cluster_version_changed;
fdb_err(1040) -> external_client_already_loaded;
fdb_err(1042) -> proxy_memory_limit_exceeded;
fdb_err(1101) -> operation_cancelled;
fdb_err(1102) -> future_released;
fdb_err(1500) -> platform_error;
fdb_err(1501) -> large_alloc_failed;
fdb_err(1502) -> performance_counter_error;
fdb_err(1510) -> io_error;
fdb_err(1511) -> file_not_found;
fdb_err(1512) -> bind_failed;
fdb_err(1513) -> file_not_readable;
fdb_err(1514) -> file_not_writable;
fdb_err(1515) -> no_cluster_file_found;
fdb_err(1516) -> file_too_large;
fdb_err(2000) -> client_invalid_operation;
fdb_err(2002) -> commit_read_incomplete;
fdb_err(2003) -> test_specification_invalid;
fdb_err(2004) -> key_outside_legal_range;
fdb_err(2005) -> inverted_range;
fdb_err(2006) -> invalid_option_value;
fdb_err(2007) -> invalid_option;
fdb_err(2008) -> network_not_setup;
fdb_err(2009) -> network_already_setup;
fdb_err(2010) -> read_version_already_set;
fdb_err(2011) -> version_invalid;
fdb_err(2012) -> range_limits_invalid;
fdb_err(2013) -> invalid_database_name;
fdb_err(2014) -> attribute_not_found;
fdb_err(2015) -> future_not_set;
fdb_err(2016) -> future_not_error;
fdb_err(2017) -> used_during_commit;
fdb_err(2018) -> invalid_mutation_type;
fdb_err(2020) -> transaction_invalid_version;
fdb_err(2021) -> no_commit_version;
fdb_err(2022) -> environment_variable_network_option_failed;
fdb_err(2023) -> transaction_read_only;
fdb_err(2024) -> invalid_cache_eviction_policy;
fdb_err(2026) -> blocked_from_network_thread;
fdb_err(2100) -> incompatible_protocol_version;
fdb_err(2101) -> transaction_too_large;
fdb_err(2102) -> key_too_large;
fdb_err(2103) -> value_too_large;
fdb_err(2104) -> connection_string_invalid;
fdb_err(2105) -> address_in_use;
fdb_err(2106) -> invalid_local_address;
fdb_err(2107) -> tls_error;
fdb_err(2108) -> unsupported_operation;
fdb_err(2200) -> api_version_unset;
fdb_err(2201) -> api_version_already_set;
fdb_err(2202) -> api_version_invalid;
fdb_err(2203) -> api_version_not_supported;
fdb_err(2210) -> exact_mode_without_limits;
fdb_err(4000) -> unknown_error;
fdb_err(4100) -> internal_error.

wait(Something) ->
    wait(Something, infinity).

wait(Something, Timeout) ->
    erlfdb:wait(Something, [{timeout, Timeout}]).

commit(?IS_TX = Tx) ->
    case erlfdb:is_read_only(Tx) andalso not erlfdb:has_watches(Tx) of
        true -> erlfdb:cancel(Tx), ok;
        false -> wait(erlfdb:commit(Tx), 10000)
    end.
