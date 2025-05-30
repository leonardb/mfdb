
-record(sel, { alias                            % TODO: not used
             , tab
             , table_id
             , db
             , keypat
             , lastval
             , ms                               % TODO: not used
             , compiled_ms
             , limit
             , key_only = false                 % TODO: not used
             , direction = forward              % TODO: not used
             }).

-type on_write_error() :: debug | verbose | warning | error | fatal.
-type on_write_error_store() :: atom() | undefined.

-define(WRITE_ERR_DEFAULT, verbose).
-define(WRITE_ERR_STORE_DEFAULT, undefined).

-define(KB, 1024).
-define(MB, 1024 * 1024).
-define(GB, 1024 * 1024 * 1024).

%% FDB has limits on key and value sizes
-define(MAX_VALUE_SIZE, 92160). %% 90Kb in bytes
-define(MAX_KEY_SIZE, 9216). %% 9Kb in bytes

-define(TABLE_PREFIX, <<"tbl_">>).
-define(FDB_WC, '_').
-define(FDB_END, <<"~">>).
-define(DATA_PREFIX, <<"dd">>).
-define(DATA_PART_PREFIX, <<"pt">>).
-define(DATA_REF_PREFIX, <<"mfdb_ref">>).
-define(IDX_DATA_PREFIX, <<"id">>).
-define(IDX_COUNT_PREFIX, <<"ic">>).
-define(COUNTER_PREFIX, <<"cc">>).
-define(TABLE_COUNT_PREFIX, <<"tc">>).
-define(TABLE_SIZE_PREFIX, <<"ts">>).

-define(TTL_TO_KEY_PFX, <<"ttl-t2k">>).
-define(KEY_TO_TTL_PFX, <<"ttl-k2t">>).

-define(IS_DB, {erlfdb_database, _}).
-define(IS_TX, {erlfdb_transaction, _}).
-define(IS_FUTURE, {erlfdb_future, _, _}).
-define(IS_FOLD_FUTURE, {fold_info, _, _}).
-define(IS_SS, {erlfdb_snapshot, _}).
-define(IS_ITERATOR, {cont, #iter_st{}}).
-define(GET_TX(SS), element(2, SS)).
-define(SORT(RecName, L), mfdb_lib:sort(RecName, L)).
-define(NOOP_SENTINAL, '__nochange__').

-type fdb_db() :: {erlfdb_database, reference()}.
-type fdb_tx() :: {erlfdb_transaction, reference()}.
-type selector() :: {binary(), gteq | gt | lteq | lt} | {binary(), gteq | gt | lteq | lt, any()}.
-type idx() :: {atom(), index, {pos_integer(), atom()}}.

-type info_opt() :: all | size | count | fields | indexes | ttl.

-define(FIELD_TYPES, [binary, integer, float, list, tuple, date, datetime, time, inet, inet4, inet6, atom, any, term, undefined, null]).

-define(TABPROC(Table), {via, gproc, {n, l, {mfdb, Table}}}).
-define(REAPERPROC(Table), {via, gproc, {n, l, {mfdb_reaper, Table}}}).

-type field_ttl()   :: {field, pos_integer()}.
-type ttl_period()  :: {minutes | hours | days | unix, pos_integer()}.
-type ttl_periods() :: list(ttl_period()).
-type table_ttl()   :: {table, ttl_period()}.
-type ttl()         :: field_ttl() | table_ttl().
-type ttls()        :: list(ttl()).
-type ttl_callback() :: {atom(), atom()}. %% {module, function}
-type table_name()  :: atom().
-type field_name()  :: atom().
-type field_type()  :: binary | integer | float | list | tuple | date | datetime | time | inet | inet4 | inet6 | atom | any | term | undefined | null.
-type field()       :: {field_name(), field_type() | list(field_type())}.
-type fields()      :: [field()].
-type index()       :: pos_integer().
-type indexes()     :: [] | list(index()).
-type mfdbrecord()  :: {atom(), fields()}.
-type continuation() :: function().
-type field_changes() :: list({field(), any()}) | function().

-type option() :: {record, mfdbrecord()} |
                  {indexes, indexes()} |
                  {table_ttl, ttl()} |
                  {field_ttl, index()} |
                  {ttl_callback, ttl_callback()}.
-type options() :: list(option()).

-type watcher_callback() :: {callback, atom(), atom()}.
-type watcher_notify() :: {notify, info | cast | call}.
-type watcher_option() :: watcher_callback() | watcher_notify().

-record(conn,
        {
         id      = conn :: conn,
         key            :: undefined | binary(),    %% Used as root prefix for multi-tenancy
         key_id         :: undefined | binary(),    %% The HCA id for the Key
         cluster        :: binary(),                %% Absolute path, incl filename, of fdb.cluster file
         tls_ca_path    :: undefined | binary(),    %% Absolute path, incl filename, of CA certificate
         tls_key_path   :: undefined | binary(),    %% Absolute path, incl filename, of Private Key
         tls_cert_path  :: undefined | binary()     %% Absolute path, incl filename, of SSL Certificate
        }).

-record(idx,
        {
         pos        :: pos_integer(),
         hca_id     :: binary(),
         data_key   :: binary(), %% Index Data Key:  <<"id", (bit_size(IdxHcaId)):8, IdxHcaId/binary>>
         count_key  :: binary()  %% Index Count Key: <<"ic", (bit_size(IdxHcaId)):8, IdxHcaId/binary>>
        }).

-record(st,
        {
         tab                            :: binary(),
         key_id                         :: binary(), %% Used as root prefix for multi-tenancy
         alias                          :: atom(),
         record_name                    :: atom(),
         fields                         :: list(field()),
         index                          :: tuple(), %% a tuple of {undefined | #idx{}, ...}
         db                             :: fdb_db() | fdb_tx(),
         table_id                       :: binary(),
         pfx                            :: binary(), %% <<(bit_size(KeyId) + bit_size(TableId) + 16):8, (bit_size(KeyId)):8, KeyId/binary, (bit_size(TableId)):8, TableId/binary>>,
         hca_ref,   %% opaque :: #erlfdb_hca{} record used for mfdb_part() keys    :: erlfdb_hca:create(<<"parts_", TableId/binary>>).
         info           = [],
         ttl            = undefined     :: undefined | ttl(),
         write_lock     = false         :: boolean(),
         counters       = #{}           :: #{binary() => integer()} %% Map #{<<"counter key">> => integer(shard count)}
        }).
%% rd(st, {tab,key_id,alias,record_name,fields,index,db,table_id,pfx,hca_ref,info,ttl,write_lock,counters}).
-type st() :: #st{}.

-record(info, {k, v}).

-record(iter_st,
        {
         db :: fdb_db(),
         tx :: undefined | fdb_tx(),
         pfx :: binary(),
         data_count = 0 :: non_neg_integer(),
         data_limit = 0 :: non_neg_integer(),
         data_acc = [],
         data_fun :: undefined | function(),
         keys_only = false :: boolean(),
         compiled_ms :: undefined | ets:comp_match_spec(),
         start_key :: any(),
         start_sel :: selector(),
         end_sel :: selector(),
         limit = 10000 :: pos_integer(),
         target_bytes :: integer(),
         streaming_mode :: atom(),
         iteration :: pos_integer(),
         snapshot :: boolean(),
         reverse :: 1 | 0
        }).
