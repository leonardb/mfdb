A mnesia-like record layer for FoundationDb
===

`mfdb` provides a mnesia-like query layer over FoundationDB.

This project supersedes https://github.com/leonardb/mnesia_fdb

It supports:
- strict typing on record fields
- secondary indexes
- table size (bytes) `mfdb:table_info(Tab, size)`
- table count `mfdb:table_info(Tab, count)`
- per table or per record TTLs

**WARNING** Unlike mnesia, keypos cannot be specified and the key is always the first field of the record.

# Table creation options

- ###record :: {RecordName :: atom, Fields :: list({atom(), atom() list(atom())})}.

    Fields which are typed are checked for validity on insert.
EG: `{record, {test, [{id, integer}, {value, [undefined | binary]}]}}`

    Supported types for fields:
    
    ```erlang
    -type field_type() :: binary | integer | float | list | tuple | date | datetime | time | inet | inet4 | inet6 | atom | any | term | undefined | null.
  ```

- ###indexes :: list(integer())
    This is like mnesia's `index` option.

    Indexed fields have an enforced size limitation of 9kb due to FoundationDB key size constraints.
    
    Secondary indexes are used automatically in `mfdb:select/2` (if they exist) and can be used explicitly through `mfdb:index_read/3`

- ###TTLs
    - ####table_ttl :: {table_ttl, ttl()}
        ```erlang
        -type ttl() :: {minutes | hours | days, pos_integer()}.
      ```
    *or*
    - ####field_ttl :: {field_ttl, integer()}
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

