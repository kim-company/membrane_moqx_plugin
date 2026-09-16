#!/usr/bin/env sh
set -eu

cert_dir=${1:-.tmp/integration-certs}
mkdir -p "$cert_dir"
cd "$cert_dir"

if [ -s ca.pem ] && [ -s server.pem ] && [ -s server-key.pem ] && \
  openssl x509 -checkend 2592000 -noout -in server.pem >/dev/null 2>&1; then
  exit 0
fi

cat >openssl.cnf <<'EOF'
[req]
distinguished_name = dn
req_extensions = ext
prompt = no
[dn]
CN = localhost
[ext]
keyUsage = keyEncipherment, dataEncipherment, digitalSignature
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @names
[names]
DNS.1 = localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

openssl genrsa -out ca-key.pem 2048
openssl req -x509 -new -key ca-key.pem -sha256 -days 36500 \
  -subj "/CN=membrane_moqx_plugin loopback CA" -out ca.pem
openssl genrsa -out server-key.pem 2048
openssl req -new -key server-key.pem -out server.csr -config openssl.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server.pem -days 36500 -sha256 -extensions ext -extfile openssl.cnf
chmod 0644 ca.pem server.pem server-key.pem
