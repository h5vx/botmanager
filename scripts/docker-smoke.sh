#!/usr/bin/env bash
# Smoke test of a built image that follows the README quickstart step by
# step: certificates from the image's own botmanager-certs, a secure node,
# /readyz, an mTLS gRPC call with a client certificate, and the dev-mode
# one-liner. Usage: scripts/docker-smoke.sh IMAGE
set -euo pipefail

IMAGE=${1:?usage: $0 IMAGE}
WORK=$(mktemp -d)
NAME=botmanager-smoke-$$
cleanup() {
	docker rm -f "$NAME" "$NAME-dev" >/dev/null 2>&1 || true
	rm -rf "$WORK"
}
trap cleanup EXIT
cd "$WORK"
U="$(id -u):$(id -g)"

wait_ready() {
	for _ in $(seq 1 60); do
		if curl -fsS "http://127.0.0.1:$1/readyz" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	echo "node on port $1 did not become ready" >&2
	docker logs "$2" >&2 || true
	return 1
}

echo "--- certificates"
mkdir -p certs data
certs() { docker run --rm --user "$U" -v "$PWD/certs:/certs" --entrypoint /app/botmanager-certs "$IMAGE" "$@"; }
certs init -out /certs
certs issue -out /certs -name node -hosts localhost,127.0.0.1
certs issue -out /certs -name client
ls certs

echo "--- secure node"
docker run -d --name "$NAME" --user "$U" \
	-p 9090:9090 -p 9091:9091 \
	-v "$PWD/certs:/etc/botmanager/certs:ro" \
	-v "$PWD/data:/var/lib/botmanager" \
	"$IMAGE" >/dev/null
wait_ready 9091 "$NAME"
curl -fsS http://127.0.0.1:9091/readyz
echo

# grpcurl runs as the same user as everything else here: the key files are
# 0600 and owned by that user, exactly as on the host in the README.
grpc() {
	docker run --rm --network host --user "$U" -v "$PWD/certs:/certs:ro" fullstorydev/grpcurl:latest "$@"
}
mtls=(-cacert /certs/ca.pem -cert /certs/client.pem -key /certs/client-key.pem)

echo "--- mTLS call"
grpc "${mtls[@]}" localhost:9090 botmanager.v1.Maintenance/GetClusterStatus | tee status.json
grep -Eq '"leader(Id|_id)": "node-1"' status.json

echo "--- plaintext call must be rejected"
if grpc -plaintext localhost:9090 list >/dev/null 2>&1; then
	echo "plaintext call was accepted" >&2
	exit 1
fi

echo "--- token not stored in plaintext"
grpc "${mtls[@]}" -d '{"display_name": "smoke", "token": "123456:SMOKETESTSMOKETESTSMOKE"}' \
	localhost:9090 botmanager.v1.BotAdmin/CreateBot >/dev/null
if grep -rq SMOKETESTSMOKETESTSMOKE data; then
	echo "plaintext token found in the data directory" >&2
	exit 1
fi
docker rm -f "$NAME" >/dev/null

echo "--- dev mode one-liner"
docker run -d --name "$NAME-dev" -p 9091:9091 -e BOTMANAGER_SECURITY__INSECURE=true "$IMAGE" >/dev/null
wait_ready 9091 "$NAME-dev"
echo "smoke test passed"
