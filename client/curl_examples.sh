#!/bin/bash

# publish a message
curl -X POST localhost:8000/publish \
  -H "Content-Type: application/json" \
  -d '{"key":"foo","value":"hello"}'

# consume from offset 0
curl "localhost:8000/consume?offset=0"
