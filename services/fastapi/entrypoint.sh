#!/bin/bash

set -e

host="rabbitmq"
port="15672"
cmd="$@"

>&2 echo "!!!!!!!! Check Rabbitmq for available !!!!!!!!"

until curl http://"$host":"$port"; do
  >&2 echo "Rabbitmq is unavailable - sleeping"
  sleep 3
done

>&2 echo "Rabbitmq is up - executing command"

exec $cmd