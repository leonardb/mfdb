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
         table_count/1,
         table_data_size/1,
         update_counter/4,
         set_counter/4,
         validate_reply_/1,
         unixtime/0,
         unixminute/0,
         expired/1,
         sort/1,
         sort/2]).

-export([clear_table/3]).

-export([flow/2,
         validate_record/1,
         validate_indexes/2,
         check_field_types/2,
         check_index_sizes/2]).

-include("mfdb.hrl").

-define(SECONDS_TO_EPOCH, (719528*24*3600)).
-define(ENTRIES_PER_COUNTER, 50).

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
%% insert or replace a record. If a record is unchanged it is not updated
%% in order to reduce the load potentially added by indexes
%% @end
put(#st{db = ?IS_DB = Db, pfx = TabPfx, hca_ref = HcaRef, index = Indexes, ttl = Ttl}, PkValue, Record) ->
    %%Tx = erlfdb:create_transaction(Db),
    %% Operation is on a data table
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              EncKey = encode_key(TabPfx, {?DATA_PREFIX, PkValue}),
              EncRecord = term_to_binary(Record),
              Size = byte_size(EncRecord),
              {DoSave, SizeInc, CountInc} =
                  case erlfdb:wait(erlfdb:get(Tx, EncKey)) of
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
                                  ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
                                  %% Replacing entry, only changes the size
                                  {true, ((OldSize * -1) + Size), 0};
                              false ->
                                  {false, 0, 0}
                          end;
                      <<OldSize:32, OldEncRecord/binary>> when OldEncRecord =/= EncRecord ->
                          %% Record exists, but has changed
                          %% Remove any old index pointers
                          ok = remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(OldEncRecord), Indexes),
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
                      put_(Tx, TabPfx, HcaRef, Size, EncKey, Size, EncRecord),
                      ok = create_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes),
                      %% Update the 'size' of stored data
                      ok = inc_counter_(Tx, TabPfx, ?TABLE_SIZE_PREFIX, SizeInc),
                      %% Update the count of stored records
                      ok = inc_counter_(Tx, TabPfx, ?TABLE_COUNT_PREFIX, CountInc),
                      %% Create/update any TTLs
                      ok = ttl_add(Tx, TabPfx, Ttl, PkValue);
                  %% and finally commit the changes
                  %%ok = erlfdb:wait(erlfdb:commit(Tx));
                  false ->
                      put_(Tx, TabPfx, HcaRef, Size, EncKey, Size, EncRecord),
                      %% Update any TTLs
                      ok = ttl_add(Tx, TabPfx, Ttl, PkValue)
                      %%ok = erlfdb:wait(erlfdb:commit(Tx))
              end
      end).

put_(Tx, TabPfx, HcaRef, Size, EncKey, Size, EncRecord)
  when Size > ?MAX_VALUE_SIZE ->
    %% Record split into multiple parts
    MfdbRefPartId = save_parts(Tx, TabPfx, HcaRef, EncRecord),
    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<"mfdb_ref", Size:32, MfdbRefPartId/binary>>));
put_(Tx, _TabPfx, _HcaRef, Size, EncKey, Size, EncRecord) ->
    ok = erlfdb:wait(erlfdb:set(Tx, EncKey, <<Size:32, EncRecord/binary>>)).

ttl_add(_Tx, _TabPfx, undefined, _Key) ->
    ok;
ttl_add(?IS_DB = Db, TabPfx, TTL, Key) ->
    Tx = erlfdb:create_transaction(Db),
    ok = ttl_add(Tx, TabPfx, TTL, Key),
    erlfdb:wait(erlfdb:commit(Tx));
ttl_add(?IS_TX = Tx, TabPfx, TTL, Key) ->
    %% We need to be able to lookup in both directions
    %% since we use a range query for reaping expired records
    %% and we also need to remove the previous entry if a record gets updated
    ok = ttl_remove(Tx, TabPfx, TTL, Key),
    Now = unixminute(), %% aligned to current minute + 1 minute
    erlfdb:wait(erlfdb:set(Tx, encode_key(TabPfx, {?TTL_TO_KEY_PFX, Now, Key}), <<>>)),
    erlfdb:wait(erlfdb:set(Tx, encode_key(TabPfx, {?KEY_TO_TTL_PFX, Key}), integer_to_binary(Now, 10))).

ttl_remove(_Tx, _TabPfx, undefined, _Key) ->
    ok;
ttl_remove(?IS_DB = Db, TabPfx, TTL, Key) ->
    Tx = erlfdb:create_transaction(Db),
    ok = ttl_remove(Tx, TabPfx, TTL, Key),
    erlfdb:wait(erlfdb:commit(Tx));
ttl_remove(?IS_TX = Tx, TabPfx, _TTL, Key) ->
    TtlK2T = encode_key(TabPfx, {?KEY_TO_TTL_PFX, Key}),
    case erlfdb:wait(erlfdb:get(Tx, TtlK2T)) of
        not_found ->
            ok;
        Added ->
            OldTtlT2K = {?TTL_TO_KEY_PFX, binary_to_integer(Added, 10), Key},
            erlfdb:wait(erlfdb:clear(Tx, encode_key(TabPfx, OldTtlT2K)))

    end,
    erlfdb:wait(erlfdb:clear(Tx, encode_key(TabPfx, {?KEY_TO_TTL_PFX, Key}))).

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
    inc_counter_(Tx, TabPfx, {IdxCountKey, Value}, 1),
    ok = erlfdb:wait(erlfdb:set(Tx, encode_key(TabPfx, {IdxDataKey, {Value, PkValue}}), <<>>)),
    create_any_indexes_(Tx, TabPfx, PkValue, Record, Rest).

parts_value_(MfdbRefPartId, TabPfx, Tx) ->
    {?DATA_PART_PREFIX, PartHcaVal} = sext:decode(MfdbRefPartId),
    Start = encode_prefix(TabPfx, {?DATA_PART_PREFIX, PartHcaVal, <<"_">>, ?FDB_WC}),
    Parts = erlfdb:wait(erlfdb:get_range_startswith(Tx, Start)),
    bin_join_parts(Parts).

delete(#st{db = ?IS_DB = Db} = St, PkValue) ->
    %% deleting a data item
    Tx = erlfdb:create_transaction(Db),
    ok = delete(St#st{db = Tx}, PkValue),
    erlfdb:wait(erlfdb:commit(Tx));
delete(#st{db = ?IS_TX = Tx, pfx = TabPfx, index = Indexes}, PkValue) ->
    %% deleting a data item
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
                {?DATA_PART_PREFIX, PartHcaVal} = sext:decode(MfdbRefPartId),
                Start = encode_prefix(TabPfx, {?DATA_PART_PREFIX, PartHcaVal, <<"_">>, ?FDB_WC}),
                ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Start)),
                {OldSize * -1, -1};
            <<OldSize:32, EncRecord/binary>> ->
                %% Remove any index values that may exist
                ok = remove_any_indexes_(Tx, TabPfx, PkValue, binary_to_term(EncRecord), Indexes),
                {OldSize * -1, -1}
        end,
    %% decrement size
    inc_counter_(Tx, TabPfx, ?TABLE_SIZE_PREFIX, IncSize),
    %% decrement item count
    inc_counter_(Tx, TabPfx, ?TABLE_COUNT_PREFIX, IncCount),
    ok = erlfdb:wait(erlfdb:clear(Tx, EncKey)).

remove_any_indexes_(Tx, TabPfx, PkValue, Record, Indexes) when is_tuple(Indexes) ->
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, tuple_to_list(Indexes));
remove_any_indexes_(_Tx, _TabPfx, _PkValue, _Record, []) ->
    ok;
remove_any_indexes_(Tx, TabPfx, PkValue, Record, [undefined | Rest]) ->
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, Rest);
remove_any_indexes_(Tx, TabPfx, PkValue, Record, [#idx{pos = Pos, data_key = IdxDataKey, count_key = IdxCountKey} | Rest]) ->
    Value = element(Pos, Record),
    inc_counter_(Tx, TabPfx, {IdxCountKey, Value}, -1),
    ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, encode_prefix(TabPfx, {IdxDataKey, {Value, PkValue}}))),
    remove_any_indexes_(Tx, TabPfx, PkValue, Record, Rest).

idx_matches(#st{db = Db, index = Indexes, pfx = TabPfx}, IdxPos, Value) ->
    #idx{count_key = CountPfx} = element(IdxPos, Indexes),
    Pfx = encode_prefix(TabPfx, {CountPfx, Value, ?FDB_WC}),
    case erlfdb:get_range_startswith(Db, Pfx) of
        [] ->
            0;
        KVs ->
            lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
    end.

table_data_size(#st{db = Db, pfx = TabPfx}) ->
    Pfx = encode_prefix(TabPfx, {?TABLE_SIZE_PREFIX, ?FDB_WC}),
    case erlfdb:get_range_startswith(Db, Pfx) of
        [] ->
            0;
        KVs ->
            lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
    end.

table_count(#st{db = Db, pfx = TabPfx}) ->
    Pfx = encode_prefix(TabPfx, {?TABLE_COUNT_PREFIX, ?FDB_WC}),
    case erlfdb:get_range_startswith(Db, Pfx) of
        [] ->
            0;
        KVs ->
            lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
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

save_parts(?IS_TX = Tx, TabPfx, Hca, Bin) ->
    PartId = erlfdb_hca:allocate(Hca, Tx),
    PartKey = sext:encode({?DATA_PART_PREFIX, PartId}),
    ok = save_parts_(Tx, TabPfx, PartId, 0, Bin),
    PartKey.

save_parts_(_Tx, _TabPfx, _PartId, _PartInc, <<>>) ->
    ok;
save_parts_(Tx, TabPfx, PartId, PartInc, <<Part:?MAX_VALUE_SIZE/binary, Rest/binary>>) ->
    Key = encode_key(TabPfx, {?DATA_PART_PREFIX, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Part)),
    save_parts_(Tx, TabPfx, PartId, PartInc + 1, Rest);
save_parts_(Tx, TabPfx, PartId, PartInc, Tail) ->
    Key = encode_key(TabPfx, {?DATA_PART_PREFIX, PartId, <<"_">>, PartInc}),
    ok = erlfdb:wait(erlfdb:set(Tx, Key, Tail)),
    save_parts_(Tx, TabPfx, PartId, PartInc + 1, <<>>).

update_counter(?IS_DB = Db, TabPfx, Key, Incr) ->
    Tx = erlfdb:create_transaction(Db),
    try do_update_counter(Tx, TabPfx, Key, Incr)
    catch
        error:{erlfdb_error,1025}:_Stack ->
            error_logger:error_msg("Update counter transaction cancelled, retrying"),
            erlfdb:wait(erlfdb:cancel(Tx)),
            update_counter(Db, TabPfx, Key, Incr)
    end.

do_update_counter(Tx, TabPfx, Key, Increment) ->
    %% Increment random counter
    EncKey = encode_key(TabPfx, {?COUNTER_PREFIX, Key, rand:uniform(?ENTRIES_PER_COUNTER)}),
    erlfdb:wait(erlfdb:add(Tx, EncKey, Increment)),
    %% Read the updated counter value
    NewVal = counter_read_(Tx, TabPfx, Key),
    erlfdb:wait(erlfdb:commit(Tx)),
    NewVal.

set_counter(?IS_DB = Db, TabPfx, Key, Value) ->
    erlfdb:transactional(
      Db,
      fun(Tx) ->
              Pfx = encode_prefix(TabPfx, {?COUNTER_PREFIX, Key, ?FDB_WC}),
              EncKey = encode_key(TabPfx, {?COUNTER_PREFIX, Key, rand:uniform(?ENTRIES_PER_COUNTER)}),
              ok = erlfdb:wait(erlfdb:clear_range_startswith(Tx, Pfx)),
              ok = erlfdb:wait(erlfdb:add(Tx, EncKey, Value))
      end).

counter_read_(Tx, TabPfx, Key) ->
    Pfx = encode_prefix(TabPfx, {?COUNTER_PREFIX, Key, ?FDB_WC}),
    case erlfdb:wait(erlfdb:get_range_startswith(Tx, Pfx)) of
        [] ->
            0;
        KVs ->
            lists:sum([Count || {_, <<Count:64/signed-little-integer>>} <- KVs])
    end.

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

check_field_types([], []) ->
    true;
check_field_types([_Val | RestVal], [Field | RestFields]) when is_atom(Field) ->
    check_field_types(RestVal, RestFields);
check_field_types([Val | RestVal], [{Field, Type} | RestFields]) ->
    case type_check_(Val, Type) of
        true ->
            check_field_types(RestVal, RestFields);
        false ->
            {error, {Field, Val, list_to_atom("not_a_" ++ atom_to_list(Type))}}
    end.

check_index_sizes([], []) ->
    true;
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

unixminute() ->
    {D, {H, M, _S}} = erlang:universaltime(),
    %% Ensure 'at least' a minute
    datetime_to_unix({D, {H, M, 0}}) + 60.

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

-spec expired(undefined | ttl()) -> never | integer().
expired(undefined) ->
    never; %% never expires
expired({minutes, X}) ->
    Exp = X * 60,
    {D, {H, M, _S}} = erlang:universaltime(),
    datetime_to_unix({D, {H, M, 0}}) - Exp;
expired({hours, X}) ->
    Exp = X * 60 * 60,
    {D, {H, M, _S}} = erlang:universaltime(),
    datetime_to_unix({D, {H, M, 0}}) - Exp;
expired({days, X}) ->
    Exp = X * 60 * 60 * 24,
    {D, {H, M, _S}} = erlang:universaltime(),
    datetime_to_unix({D, {H, M, 0}}) - Exp.
