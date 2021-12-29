A mnesia-like record layer for FoundationDb
===

`mfdb` provides a mnesia-like query layer over FoundationDB.

This project supersedes https://github.com/leonardb/mnesia_fdb

It supports:
- optional basic type checking on record fields
- secondary indexes (caveat, 9Kb field size limit for indexed fields)
- table size (bytes) `mfdb:table_info(Tab, size)`
- table count `mfdb:table_info(Tab, count)`
- per table TTL *or* per record TTLs

**WARNING** Unlike mnesia, keypos cannot be specified and the key is always the first field of the record.

## Requirements
 - FoundationDB 6.2.x Server and Client: https://www.foundationdb.org/download/
 - `couchdb-erlfdb`: https://github.com/leonardb/couchdb-erlfdb This is forked from https://github.com/apache/couchdb-erlfdb with changes to support rebar3, alternate build, dialyzer fixes, and custom native types.

# Table creation options

- ###record :: {RecordName :: atom, Fields :: list({atom(), atom() list(atom())})}.

    Fields which are typed are checked for validity on insert.
EG: `{record, {test, [{id, integer}, {value, [undefined | binary]}, {expires, datetime}]}}`

    Supported types for fields:
    
    ```erlang
    -type field_type() :: binary | integer | float | list | tuple | date | datetime | time | inet | inet4 | inet6 | atom | any | term | undefined | null.
  ```
  
    When a field is configured as the TTL field, the field type _must_ be 'datetime'. This field automatically supports the atom 'never'.

- ###indexes :: list(integer())
    This is like mnesia's `index` option.

    Indexed fields have an enforced size limitation of 9kb due to FoundationDB key size constraints.
    
    Secondary indexes are used automatically in `mfdb:select/2` (if they exist) and can be used explicitly through `mfdb:index_read/3`

- ###TTLs
    ```erlang
    -type field_ttl()   :: {field, pos_integer()}. %% where the post_integer is the field position in the record
    -type ttl_period()  :: {minutes | hours | days | unix, pos_integer()}.
    -type table_ttl()   :: {table, ttl_period()}.
    -type ttl()         :: field_ttl() | table_ttl().
  ```
    - ####table_ttl :: {table_ttl, table_ttl()}
    *or*
    - ####field_ttl :: {field_ttl, field_ttl()}
        where the integer is the index of the field to use for TTLs.
        
        This field *must* be of type 'datetime' and expects a UTC calendar:datetime() or the atom 'never'. 
  
        When TTL is configured for a table older records will be reaped automatically.
  
        Any insert operation will reset the TTL for a record.

# Counters

Atomic integer counters are supported as a special type on tables.

Counters can be 'set' to a value, and incremented/decremented

```erlang
ok = mfdb:set_counter(test, mycounter, -100).
0 = mfdb:update_counter(test, my_counter, 100).
1 = mfdb:update_counter(test, my_counter, 1).
5 = mfdb:update_counter(test, my_counter, 4).
-5 = mfdb:update_counter(test, my_counter, -10).
```

# Fold Operations

mfdb supports both a full table fold and folding using a match-spec.

```erlang
-type dbrec() :: tuple(). %% erlang record
-type table_name()  :: atom().
-type foldfun() :: fun((tx(), dbrec(), any()) -> any()) | fun((dbrec(), any()) -> any()).

-spec fold(Table :: table_name(), InnerFun :: foldfun(), OuterAcc :: any()) -> any().

-spec fold(Table :: table_name(), InnerFun :: foldfun(), OuterAcc :: any(), MatchSpec :: ets:match_spec()) -> any().
```

When called with a two-arity `InnerFun`, the fold operation is read-only.
```erlang
InnerFun = fun(#test{id = Id} = Rec, Acc) ->
               case (Id rem 2) =:= 0 of
                  true ->
                      [Rec | Acc];
                  false ->
                      Acc
               end
            end.
```

When called with a three-arity `InnerFun` the fold takes and explicit read and write lock on each matching record before passing it into `InnerFun`.
The following operations are then available within the context of the transaction:
- `mfdb:delete(Tx, PrimaryKey)`
- `mfdb:insert(Tx, Record)`
- `mfdb:update(Tx, PrimaryKey, Changes)`

Each step is committed individually and failed commits are silently rolled back/discarded.

This allows for multiple processes to be folding over the same data allowing changes to be applied.

There is an example of a parallel fold/delete process in the tests.

```erlang
InnerFun = fun(Tx, #test{id = Id} = Rec, Acc) ->
               case (Id rem 2) =:= 0 of
                  true ->
                      ok = mfdb:delete(Tx, Id),
                      [Rec | Acc];
                  false ->
                      Acc
               end
            end.
```
