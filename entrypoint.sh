#!/bin/bash

echo "Service: ${SERVICE_NAME}"
IP=$(hostname -i)
export IP
echo "IP: ${IP}"

terminate() {
  echo "Termination signal received, shutting down..."
  kill -SIGTERM "$UVICORN_PID"
  wait "$UVICORN_PID"
  echo "Uvicorn has been terminated"
}

trap terminate SIGTERM SIGINT

uvicorn app_delivery.main:app \
  --host 0.0.0.0 \
  --port 5002 \
  --ssl-keyfile /certs/delivery/delivery-key.pem \
  --ssl-certfile /certs/delivery/delivery-cert.pem \
  --ssl-ca-certs /certs/ca.pem \
  --ssl-cert-reqs 2 &

UVICORN_PID=$!

wait "$UVICORN_PID"