FROM ubuntu:14.04
# Install the unzip utility.
RUN http_proxy='http://cloud-proxy:3128' DEBIAN_FRONTEND=noninteractive apt-get -y update
RUN http_proxy='http://cloud-proxy:3128' DEBIAN_FRONTEND=noninteractive apt-get -y install python-psycopg2

# Add files
ADD ./defaults.json ./
ADD ./local.json ./
ADD ./settings.py ./
ADD ./tcga_import.py ./

# Set the entrypoint to be a consul agent.
ENTRYPOINT python import_tcga_dcc.py