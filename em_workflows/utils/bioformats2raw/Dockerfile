ARG BUILD_IMAGE=ubuntu:20.04

FROM ${BUILD_IMAGE}

RUN DEBIAN_FRONTEND=noninteractive \
  apt-get update -y && \
  apt-get install -y unzip wget && \
  apt-get install -y ca-certificates-java && \
  apt-get install -y openjdk-8-jdk && \
  apt-get install -y libblosc1 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN export JAVA_HOME

WORKDIR /app

ARG BIOFORMATS2RAW_VERSION=0.7.0

# setup bioformats2raw
RUN wget https://github.com/glencoesoftware/bioformats2raw/releases/download/v${BIOFORMATS2RAW_VERSION}/bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip

RUN unzip bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip && \
    rm bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip

ENV PATH="/app/bioformats2raw-${BIOFORMATS2RAW_VERSION}/bin:${PATH}"

ENTRYPOINT ["/bin/bash"]
