#!/bin/bash
#add
curl -X POST localhost:8080 -d \ '{"record": {"value": "aaaaaaaaaaaaaaaa"}}'

curl -X POST localhost:8080 -d \ '{"record": {"value": "bbbbbbbbbbbbbbbb"}}'

curl -X POST localhost:8080 -d \ '{"record": {"value": "cccccccccccccccc"}}'

#get
curl -X GET localhost:8080 -d '{"offset": 0}'
curl -X GET localhost:8080 -d '{"offset": 1}'
curl -X GET localhost:8080 -d '{"offset": 2}'


