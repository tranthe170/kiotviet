FROM --platform=linux/amd64 python:3.10.10-slim-buster

# Update and install system packages
RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev python-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install DBT
RUN pip install -U pip
# RUN pip install dbt==0.17.2
# Set environment variables
ENV DBT_DIR /dbt
# Set working directory
WORKDIR $DBT_DIR
COPY .dbt ./.dbt 
COPY requirements.txt ./
COPY cellxpert ./cellxpert
RUN pip install -r requirements.txt

WORKDIR $DBT_DIR/cellxpert
RUN dbt deps
# Run dbt
ENTRYPOINT ["dbt"]