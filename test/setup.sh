#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function restartContainers {
    docker-compose down
    docker-compose up -d
    sleep 10
}

( cd "$DIR" && restartContainers )
