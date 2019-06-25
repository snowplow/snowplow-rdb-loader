#!/bin/sh

set -e

IGLUCTL_ZIP="igluctl_0.6.0.zip"
IGLUCTL_URI="http://dl.bintray.com/snowplow/snowplow-generic/$IGLUCTL_ZIP"
SCHEMAS_PATH="$TRAVIS_BUILD_DIR/iglu-central/schemas/"

git clone https://github.com/snowplow/iglu-central.git
cd iglu-central
git checkout feature/rdb-blacklist
cd ..

docker run \
    -p 8080:8080 \
    -v $TRAVIS_BUILD_DIR/.travis:/iglu \
    --rm -d \
    snowplow-docker-registry.bintray.io/snowplow/iglu-server:0.6.0-rc16 \
    --config /iglu/server.conf

wget $IGLUCTL_URI
unzip -j $IGLUCTL_ZIP

echo "Waiting for Iglu Server..."
sleep 6

./igluctl static push \
    $SCHEMAS_PATH \
    http://localhost:8080/ \
    48b267d7-cd2b-4f22-bae4-0f002008b5ad \
    --public
