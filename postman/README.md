# Postman Integration

This folder contains live ticket API contract artifacts for ingestion readiness checks.

## Files

- `live_ticket_api.postman_collection.json`: Contract checks for the ticket endpoint.
- `environments/local.postman_environment.json`: Local development URL.
- `environments/production.template.postman_environment.json`: Production template.

## Run in Postman UI

1. Import the collection.
2. Import one environment file.
3. Select environment and run the request or collection.

## Run with Newman (CLI)

Using local Newman:

newman run postman/live_ticket_api.postman_collection.json \
  -e postman/environments/local.postman_environment.json --bail

Using Docker Newman:

docker run --rm -v "${PWD}:/etc/newman" postman/newman:alpine \
  run /etc/newman/postman/live_ticket_api.postman_collection.json \
  -e /etc/newman/postman/environments/local.postman_environment.json --bail
