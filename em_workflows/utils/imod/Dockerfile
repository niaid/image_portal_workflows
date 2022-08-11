FROM centos:8

ENV IMOD=imod_4.11.11_RHEL7-64_CUDA10.1.sh
RUN sed -i -e "s|mirrorlist=|#mirrorlist=|g" /etc/yum.repos.d/CentOS-*
RUN sed -i -e "s|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g" /etc/yum.repos.d/CentOS-*

RUN yum install -y wget file python38 libGL && ln -fs /usr/bin/python3.8 /usr/bin/python
RUN  wget https://bio3d.colorado.edu/imod/AMD64-RHEL5/${IMOD} && \
     sh ${IMOD} -yes && \
     rm -f ${IMOD}


ENV IMOD_DIR=/usr/local/IMOD
ENV PATH=$IMOD_DIR/bin:$PATH
