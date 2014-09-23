FROM ubuntu:14.04

ENV http_proxy 'http://cloud-proxy:3128'

# Install the unzip utility.
RUN DEBIAN_FRONTEND=noninteractive apt-get -y update
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install python-psycopg2
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install python-requests

# Add files
ADD ./defaults.json ./
ADD ./local.json ./
ADD ./settings.py ./
ADD ./tcga_import.py ./
ADD ./import_tcga_dcc.py ./

ENV no_proxy 'controller'

# Set the entrypoint to be a consul agent.
ENTRYPOINT python import_tcga_dcc.py