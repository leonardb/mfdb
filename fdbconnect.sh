#!/bin/bash
fdbcli -C /etc/foundationdb/fdb.cluster --tls_certificate_file /etc/foundationdb/fdb.local.pem --tls_key_file /etc/foundationdb/fdb.local.key --tls_ca_file /etc/foundationdb/fdb.local.crt --tls_verify_peers "Check.Valid=0"  --log
