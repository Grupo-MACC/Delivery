FROM python:3.12-slim-bookworm

# Configuration will be done as root
USER root

# Update pip, copy requirements file and install dependencies
RUN pip install --no-cache-dir --upgrade pip;
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# We will be working on this folder
WORKDIR /home/pyuser/code
ENV PYTHONPATH=/home/pyuser/code/app_delivery
ENV SQLALCHEMY_DATABASE_URL=sqlite+aiosqlite:///./delivery.db
ENV RABBITMQ_USER=guest
ENV RABBITMQ_PASSWORD=guest
ENV RABBITMQ_HOST=10.0.11.30
ENV PUBLIC_KEY_PATH=/home/pyuser/code/auth_public.pem
# Consul Service Discovery
ENV CONSUL_HOST=10.0.11.40
ENV CONSUL_PORT=8500
ENV SERVICE_NAME=delivery
ENV SERVICE_PORT=5002
ENV SERVICE_ID=delivery
ENV CONSUL_SCHEME=https
ENV CONSUL_CA_FILE=/certs/ca.pem
ENV CONSUL_REGISTRATION_EVENT_URL=http://54.225.33.0:8081/restart

ENV SERVICE_CERT_FILE=/certs/delivery/delivery-cert.pem
ENV SERVICE_KEY_FILE=/certs/delivery/delivery-key.pem
# Create a non root user
RUN useradd -u 1000 -d /home/pyuser -m pyuser && \
    chown -R pyuser:pyuser /home/pyuser

# Copy the entrypoint script (executed when the container starts) and add execution permissions
COPY entrypoint.sh /home/pyuser/code/entrypoint.sh
RUN chmod +x /home/pyuser/code/entrypoint.sh

# Switch user so container is run as non-root user
USER 1000

# Copy the app to the container
COPY app_delivery /home/pyuser/code/app_delivery

# Run the application
ENTRYPOINT ["/home/pyuser/code/entrypoint.sh"]


