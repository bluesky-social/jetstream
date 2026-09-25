#!/bin/sh
# Provisions the dev object stores and checks that the application credentials
# have exactly the S3 permissions production grants: PutObject, GetObject, and
# DeleteObject on one bucket, nothing else (design doc §5.2). `just up` fails if
# any check fails, so a store upgrade that silently widens or breaks access is
# caught before any code runs against it.
#
# Must be idempotent: `docker compose up` re-runs this container on every
# `just up`, including against stores that are already provisioned.
set -eu

bucket=jetstream
app_user=jetstream
app_secret=jetstream-dev-secret
region=us-east-1

mc() { command mc --quiet --no-color "$@"; }

mc alias set seaweedfs http://seaweedfs:8333 admin admin-dev-secret >/dev/null
mc alias set minio http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

# SeaweedFS users and grants are static (seaweedfs-s3.json); only the bucket
# needs creating. MinIO users and policies live in its (tmpfs) data dir.
mc mb --ignore-existing "seaweedfs/${bucket}" >/dev/null
mc mb --ignore-existing "minio/${bucket}" >/dev/null
mc admin policy create minio jetstream-app /devenv/minio-policy.json >/dev/null
mc admin user add minio "${app_user}" "${app_secret}" >/dev/null
case "$(mc admin policy entities --user "${app_user}" minio)" in
*jetstream-app*) ;;
*) mc admin policy attach minio jetstream-app --user "${app_user}" >/dev/null ;;
esac

fail() {
    echo "s3-init: $*" >&2
    exit 1
}

# s3 METHOD URL [curl args...] prints the HTTP status of a SigV4-signed request
# made with the application credentials.
s3() {
    method=$1 url=$2
    shift 2
    curl -sS -o /dev/null -w '%{http_code}' -X "${method}" \
        --aws-sigv4 "aws:amz:${region}:s3" --user "${app_user}:${app_secret}" \
        "$@" "${url}"
}

check() {
    name=$1 endpoint=$2
    key="devenv-probe/$(cat /proc/sys/kernel/random/uuid)"
    url="${endpoint}/${bucket}/${key}"
    body="probe ${key}"

    code=$(s3 PUT "${url}" --data-binary "${body}")
    [ "${code}" = 200 ] || fail "${name}: PutObject returned ${code}, want 200"

    got=$(curl -sS --fail --aws-sigv4 "aws:amz:${region}:s3" --user "${app_user}:${app_secret}" "${url}") ||
        fail "${name}: GetObject failed"
    [ "${got}" = "${body}" ] || fail "${name}: GetObject returned different bytes"

    code=$(curl -sS -o /dev/null -w '%{http_code}' "${url}")
    [ "${code}" = 403 ] || fail "${name}: anonymous GetObject returned ${code}, want 403"

    code=$(s3 GET "${endpoint}/${bucket}?list-type=2")
    [ "${code}" = 403 ] || fail "${name}: ListObjectsV2 returned ${code}, want 403 (listing must stay denied)"

    code=$(s3 PUT "${endpoint}/${bucket}-denied")
    [ "${code}" = 403 ] || fail "${name}: CreateBucket returned ${code}, want 403"

    code=$(s3 DELETE "${url}")
    [ "${code}" = 204 ] || fail "${name}: DeleteObject returned ${code}, want 204"

    code=$(s3 DELETE "${url}")
    [ "${code}" = 204 ] || fail "${name}: DeleteObject of a missing key returned ${code}, want 204"

    echo "s3-init: ${name}: bucket ${bucket} ready; app credentials limited to Put/Get/DeleteObject"
}

check seaweedfs http://seaweedfs:8333
check minio http://minio:9000
