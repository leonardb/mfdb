

## Prefix Structure

- KeyId :: HcaId linked to a UUID
- TableId :: HcaId linked to a table name

The 'key' (UUID) serves as a tenancy identifier

The base prefix is a combination of the KeyId and TableId

```erlang
#st{pfx = Prefix, key_id = KeyId, table_id = TableId} = St,
%% The Pfx is a pre-calculated binary from Key and Table Ids
Prefix =:= <<(byte_size(KeyId) + byte_size(TableId) + 2), (byte_size(KeyId)), KeyId/binary, (byte_size(TableId)), TableId/binary>>,
```

When composing the key we use 'type' identifiers.

```erlang
Data        = <<"dd">>.
DataCount   = <<"dc">>.
DataIndex   = <<"id", IdxPfx/binary>>.
IndexCount  = <<"ic", IdxPfx/binary>>.
Part        = <<"pt">>.
```

Data Key composition:
```erlang
EncPart = sext:encode({<<"dd">>, PkValue}),
Key = <<Prefix/binary,
        EncPart/binary>>
```

Then indexes and counters have additional HcaId components

Secondary Data Index composition:
```erlang
#idx{pfx = IdxPfx} = Index,
EncPart = sext:encode({IdxPfx, {FieldValue, PkValue}}),
Key = <<Prefix/binary,
        EncPart/binary>>
```


