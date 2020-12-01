A mnesia-like record layer for FoundationDb
===

`mfdb` provides a mnesia-like query layer over FoundationDB.

This project supersedes https://github.com/leonardb/mnesia_fdb

It supports:
- strict typing on record fields
- secondary indexes
- table size (bytes) `mfdb:table_info(Tab, size)`
- table count `mfdb:table_info(Tab, count)`

# Table creation options

- record :: {RecordName :: atom, Fields :: list(atom() | {atom(), atom()})}.

    Fields which are typed are checked for validity on insert.
EG: `{record, {test, [{id, integer}, {value, binary}]}}`

    Supported types for fields:
    
    ```erlang
    -type field_type() :: binary | integer | float | list | tuple | date | datetime | time | inet | inet4 | inet6 | atom | any | term | undefined | null.
  ```

- indexes :: list(integer())
    This is like mnesia's `index` option.

    Indexed fields have an enforced size limitation of 9kb due to FoundationDB key size constraints.
    
    Secondary indexes are used automatically in `mfdb:select/2` (if they exist) and can be used explicitly through `mfdb:index_read/3`

- ttl :: {ttl, ttl()}
    ```erlang
    -type ttl() :: {minutes | hours | days, pos_integer()}.
  ```
  
  When TTL is configured for a table older records will be reaped automatically.
  
  Any insert operation will reset the TTL for a record.

# Counters

Atomic counters are supported as a special type on tables.

Negative counters are not supported.

IE counters can be incremented and decremented, but have a floor of zero.  

```erlang
1 = mfdb:update_counter(test, my_counter, 1).
5 = mfdb:update_counter(test, my_counter, 4).
0 = mfdb:update_counter(test, my_counter, -10).
```
