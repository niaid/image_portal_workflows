ARG BUILD_IMAGE=ubuntu:20.04

FROM ${BUILD_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update -y && \
  apt install -y unzip wget && \
  apt install -y ca-certificates-java && \
  apt install -y openjdk-8-jdk && \
  apt install -y libblosc1 && \
  apt clean && \
  rm -rf /var/lib/apt/lists/*;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN export JAVA_HOME

ARG BIOFORMATS2RAW_VERSION=0.7.0

WORKDIR /opt/bin

# setup bioformats2raw
RUN wget https://github.com/glencoesoftware/bioformats2raw/releases/download/v${BIOFORMATS2RAW_VERSION}/bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip

RUN unzip bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip && \
    rm bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip

ENV PATH="/opt/bin/bioformats2raw-${BIOFORMATS2RAW_VERSION}/bin:${PATH}"

ENTRYPOINT ["/bin/bash"]
