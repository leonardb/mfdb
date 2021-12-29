#!/bin/bash
fdbcli -C /opt/mfdb/test/fdb.local.cluster --tls_certificate_file /opt/mfdb/test/fdb.local.pem --tls_ca_file /opt/mfdb/test/fdb.local.crt --tls_key_file /opt/mfdb/test/fdb.local.key
