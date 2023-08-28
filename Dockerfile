ARG BUILD_IMAGE=ubuntu:20.04

FROM ${BUILD_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt update -y && \
  apt install -y python3 unzip wget git python3-pip && \
  apt install -y ca-certificates-java && \
  apt install -y openjdk-8-jdk && \
  apt install -y libblosc1 && \
  apt install -y ffmpeg && \
  apt install -y imagemagick graphicsmagick && \
  apt clean && \
  rm -rf /var/lib/apt/lists/*;

RUN ln -fs /usr/bin/python3.8 /usr/bin/python

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN export JAVA_HOME

# setup IMOD
# Install libGL ?
ARG IMOD=imod_4.11.24_RHEL7-64_CUDA10.1.sh

RUN wget https://bio3d.colorado.edu/imod/AMD64-RHEL5/${IMOD} && \
    sh ${IMOD} -yes && \
    rm -f ${IMOD}

ENV IMOD_DIR=/usr/local/IMOD
ENV PATH=$IMOD_DIR/bin:$PATH

ARG BIOFORMATS2RAW_VERSION=0.7.0

WORKDIR /opt/bin
# setup bioformats2raw
RUN wget https://github.com/glencoesoftware/bioformats2raw/releases/download/v${BIOFORMATS2RAW_VERSION}/bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip
RUN unzip bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip && \
    rm bioformats2raw-${BIOFORMATS2RAW_VERSION}.zip

ENV PATH="/opt/bin/bioformats2raw-${BIOFORMATS2RAW_VERSION}/bin:${PATH}"

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN rm requirements.txt

WORKDIR /image_portal_workflows

ENTRYPOINT ["/bin/bash"]
