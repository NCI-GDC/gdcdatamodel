FROM ubuntu:14.04

ENV http_proxy 'http://cloud-proxy:3128'

# Install the unzip utility.
RUN DEBIAN_FRONTEND=noninteractive apt-get -y update
# RUN DEBIAN_FRONTEND=noninteractive apt-get -y install unzip



# EXPOSE 80

# Set the entrypoint to be a simple agent.
# ENTRYPOINT ["python", "gdc_data_import/prototyping/import_bio_archives.py"]