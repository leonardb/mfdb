%%----------------------------------------------------------------
%% Copyright (c) 2013-2016 Klarna AB
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%----------------------------------------------------------------
%% Support of FoundationDB:
%%  Copyright 2020 Leonard Boyce <leonard.boyce@lucidlayer.com>

-module(mfdb_manager).

-behaviour(gen_server).

-export([st/1]).

-export([read_info/3]).

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([create_key_/0]).

-include("mfdb.hrl").

create_key_() ->
    %% Create a key which is needed for access
    %% This is purely an admin function and
    %% should only ever be manually run
    %% Eventually should be removed
    gen_server:call(?MODULE, create_key).

st(Tab) when is_atom(Tab) ->
    st(atom_to_binary(Tab));
st(Tab) ->
    case ets:lookup(?MODULE, Tab) of
        [#st{} = St] ->
            St;
        _ ->
            badarg
    end.

read_info(Tab, Key, Default) when is_atom(Tab) ->
    case ets:lookup(?MODULE, {Tab, Key}) of
        [#info{v = Val}] ->
            Val;
        [] ->
            Default
    end.

%%%% Genserver
start_link() ->
    case ets:info(?MODULE, name) of
        undefined ->
            ets:new(?MODULE, [ordered_set, public, named_table, {keypos, 2}]);
        _ ->
            ok
    end,
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    process_flag(trap_exit, true),
    ok = mfdb_conn:init(),
    [#conn{key = Key} = Conn] = ets:lookup(?MODULE, conn),
    Db = mfdb_conn:connection(Conn),
    EncKey = sext:encode({<<"keys">>, Key}),
    case erlfdb:get(Db, EncKey) of
        not_found ->
            %% Create a new hca for the key
            Hca = erlfdb_hca:create(<<"hca_keys">>),
            KeyHcaId = erlfdb_hca:allocate(Hca, Db),
            ok = erlfdb:set(Db, EncKey, KeyHcaId),
            %% Update the #conn{} record with the key hca id
            ets:insert(?MODULE, Conn#conn{key_id = KeyHcaId});
        KeyHcaId ->
            %% Update the #conn{} record with the key hca id
            ets:insert(?MODULE, Conn#conn{key_id = KeyHcaId})
    end,
    {ok, []}.

handle_call({connect, Tab}, _From, S) ->
    R = load_table_(atom_to_binary(Tab)),
    {reply, R, S};
handle_call({delete_table, Tab0}, _From, S) ->
    R = delete_table_(atom_to_binary(Tab0)),
    {reply, R, S};
handle_call({create_table, Table, Record, Indexes}, _From, S) ->
    R = create_table_(atom_to_binary(Table), Record, Indexes),
    {reply, R, S};
handle_call(_, _, S) ->
    {reply, error, S}.

handle_cast(_, S) ->
    {noreply, S}.

handle_info(_UNKNOWN, St) ->
    {noreply, St}.

terminate(_, _) ->
    ok.

code_change(_, S, _) ->
    {ok, S}.

delete_table_(Tab) ->
    case ets:lookup(?MODULE, Tab) of [#st{db = Db, key_id = KeyId, index = Indexes, pfx = TblPfx}] ->
            ok = mfdb_lib:clear_table(Db, TblPfx, Indexes),
            TabKey = sext:encode({KeyId, <<"table">>, Tab}),
            ok = erlfdb:clear(Db, TabKey),
            ets:select_delete(?MODULE, [{#info{k = {Tab, '_'}, _ = '_'}, [], [true]}]),
            ets:select_delete(?MODULE, [{#info{k = {Tab, '_', '_'}, _ = '_'}, [], [true]}]),
            ets:delete(?MODULE, Tab),
            ok;
        [] ->
            {error, no_such_table}
    end.

load_table_(Tab) ->
    [#conn{key_id = KeyId} = Conn] = ets:lookup(?MODULE, conn),
    case ets:lookup(?MODULE, Tab) of
        [] ->
            %% table not yet loaded
            Db = mfdb_conn:connection(Conn),
            TabKey = sext:encode({KeyId, <<"table">>, Tab}),
            case erlfdb:get(Db, TabKey) of
                not_found ->
                    {error, no_such_table};
                EncSt ->
                    #st{} = TableSt = binary_to_term(EncSt),
                    ValidatorFun = mfdb_lib:types_validation_fun(TableSt),
                    true = ets:insert(?MODULE, TableSt#st{db = Db, validator = ValidatorFun}),
                    ok = mfdb_table_sup:add(binary_to_atom(Tab))
            end;
        [#st{}] ->
            %% table already loaded
            ok
    end.

create_table_(Tab, Record, Indexes) when is_binary(Tab) ->
    [#conn{key_id = KeyId} = Conn] = ets:lookup(?MODULE, conn),
    Db = mfdb_conn:connection(Conn),
    %% Functions must return 'ok' to continue, anything else will exit early
    Flow = [{fun mfdb_lib:validate_record/1, [Record]},
            {fun mfdb_lib:validate_indexes/2, [Indexes, Record]},
            {fun table_create_if_not_exists_/5, [Db, KeyId, Tab, Record, Indexes]}],
    mfdb_lib:flow(Flow, ok).

table_exists_(Db, TabKey) ->
    %% Does a table config exist
    erlfdb:get(Db, TabKey) =/= not_found.

table_create_if_not_exists_(Db, KeyId, Table, Record, Indexes) ->
    TabKey = sext:encode({KeyId, <<"table">>, Table}),
    TableSt = case table_exists_(Db, TabKey) of
                  false ->
                      Hca = erlfdb_hca:create(<<"hca_table">>),
                      TableId = erlfdb_hca:allocate(Hca, Db),
                      #st{} = TableSt0 = mk_tab_(Db, KeyId, TableId, Table, Record, Indexes),
                      ok = erlfdb:set(Db, TabKey, term_to_binary(TableSt0)),
                      TableSt0;
                  true ->
                      %% Read table spec from DB
                      StBin = erlfdb:get(Db, TabKey),
                      binary_to_term(StBin)
              end,
    ValidatorFun = mfdb_lib:types_validation_fun(TableSt),
    true = ets:insert(?MODULE, TableSt#st{db = Db, validator = ValidatorFun}),
    ok = mfdb_table_sup:add(binary_to_atom(Table)).

mk_tab_(Db, KeyId, TableId, Table, {RecordName, Fields}, Indexes) ->
    HcaRef = erlfdb_hca:create(<<TableId/binary, "_hca_ref">>),
    Pfx = <<(byte_size(KeyId) + byte_size(TableId) + 2),
            (byte_size(KeyId)), KeyId/binary,
            (byte_size(TableId)), TableId/binary>>,
    St0 = #st{
             tab         = Table,
             key_id      = KeyId,
             record_name = RecordName,
             fields      = Fields,
             index       = erlang:make_tuple(length(Fields) + 1, undefined),
             db          = Db,
             table_id    = TableId,
             hca_ref     = HcaRef,
             pfx         = Pfx,
             info        = []
            },
    %% Convert indexes to records and add to the table state
    create_indexes_(Indexes, St0).

create_indexes_([], #st{} = St) ->
    St;
create_indexes_([Pos | Rest], #st{db = Db, index = Index0} = St) ->
    IdxTableHca = erlfdb_hca:create(<<"hca_table">>),
    HcaId = erlfdb_hca:allocate(IdxTableHca, Db),
    Index = #idx{pos = Pos,
                 hca_id = HcaId,
                 data_key = <<?IDX_DATA_PREFIX/binary, HcaId/binary>>,
                 count_key = <<?IDX_COUNT_PREFIX/binary, HcaId/binary>>},
    create_indexes_(Rest, St#st{index = setelement(Pos, Index0, Index)}).
