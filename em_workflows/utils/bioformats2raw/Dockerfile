ARG BUILD_IMAGE=ubuntu:20.04

FROM ${BUILD_IMAGE}

RUN DEBIAN_FRONTEND=noninteractive \
  apt-get update && \
  apt-get install -y python3 unzip && \
  apt-get install -y ca-certificates-java && \
  apt-get install -y openjdk-8-jdk && \
  apt-get install -y libblosc1 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*;

# Fix certificate issues
RUN apt-get update && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN export JAVA_HOME

RUN mkdir /app

ENV BIOFORMATS2RAW_VERSION 0.7.0

COPY bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip ./app

WORKDIR /app

RUN unzip bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip

ENV PATH="/app/bioformats2raw-${BIOFORMATS2RAW_VERSION}/bin:${PATH}"

ENTRYPOINT ["/bin/bash"]
