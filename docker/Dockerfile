# Dockerfile for NewsLookout web scraping application.

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

USER root

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

WORKDIR /opt

ENV NLTK_DATA="/opt/newslookout/models/nltk"
ENV NEWSLOOKOUT_DATA="/var/cache/newslookout_data"
ENV NEWSLOOKOUT_HOME="/opt/newslookout"
ENV NEWSLOOKOUT_CONFIG="/etc/newslookout/newslookout.conf"

COPY requirements.txt /opt/newslookout/requirements.txt

# Assumes the latest NewsLookout release has been downloaded
# from the Github release page to the local directory:
COPY newslookout /opt/newslookout/bin

RUN apt-get -qq update \
	&& apt-get -y install curl bzip2 gzip software-properties-common dirmngr zip sudo net-tools tini ca-certificates locales \
	&& echo "en_US.UTF-8 UTF-8" > /etc/locale.gen  \
    && locale-gen \
    && update-ca-certificates \
	&& useradd -ms /bin/bash newslookout \
	&& apt-get -y install python3-pip \
	&& su - newslookout -c "pip3 --no-cache-dir install -r /opt/newslookout/requirements.txt" \
	&& su - newslookout -c "pip3 --no-cache-dir install flask fastapi uvicorn[standard]" \
	&& mkdir -p /var/log/newslookout \
    && mkdir -p /var/cache/newslookout_data \
    && mkdir -p /var/run/newslookout \
    && mkdir -p /etc/newslookout \
    && mkdir -p /opt/newslookout/models/nltk \
    && mkdir -p /opt/newslookout/models/bert_models \
	&& chown -cR newslookout:newslookout /opt/newslookout \
	&& chown -cR newslookout:newslookout /var/cache/newslookout_data \
	&& chown -cR newslookout:newslookout /etc/newslookout \
	&& chown -cR newslookout:newslookout /var/log/newslookout \
	&& chown -cR newslookout:newslookout /var/run/newslookout \
	&& apt -y clean all \
	&& apt autoclean \
	&& apt-get -qq -y autoremove \
	&& rm -rf /var/lib/apt/lists/* /var/apt/cache/* /var/log/dpkg.log

# copy finbert model weights
COPY models/bert_models /opt/newslookout/models/bert_models

# install the models for natural language processing:
RUN su - newslookout -c "python3 -m spacy download en_core_web_lg" \
	&& NLTK_DATA=/opt/newslookout/models/nltk su - newslookout -c "python3 -c \"import nltk; nltk.download('punkt'); nltk.download('maxent_treebank_pos_tagger'); nltk.download('reuters'); nltk.download('universal_treebanks_v20')\"" \
	&& mv -v /opt/newslookout/bin/plugins /opt/newslookout/plugins \
	&& mv -v /opt/newslookout/bin/plugins_contrib /opt/newslookout/plugins_contrib


LABEL \
    maintainer="sandeep.sandhu@gmx.com" \
    org.label-schema.build-date=$BUILD_DATE \
    org.label-schema.docker.dockerfile="/Dockerfile" \
    org.label-schema.license="Apache License 2.0" \
    org.label-schema.name="sandeep-sandhu/newslookout" \
    org.label-schema.version="2.1.0" \
    org.label-schema.url="https://github.com/sandeep-sandhu" \
    org.label-schema.vcs-type="Git"

ENV GITHUB_RELEASE_URL=https://github.com/sandeep-sandhu/NewsLookout/releases/latest

ENV PATH /opt/newslookout:/home/newslookout/.local/bin:$PATH
ENV NEWSLOOKOUT_PLUGINS="/opt/newslookout/plugins"
ENV NEWSLOOKOUT_PLUGINS_CONTRIB="/opt/newslookout/plugins_contrib"
ENV NEWSLOOKOUT_RUNDATE_FROM=$BUILD_DATE
ENV NEWSLOOKOUT_RUNDATE_TO=$BUILD_DATE
ENV NEWSLOOKOUT_LOG_LEVEL="INFO"
ENV NEWSLOOKOUT_SYSLOG_SERVER=""

COPY docker/newslookout_docker.conf $NEWSLOOKOUT_CONFIG

COPY newslookout.sh /opt/newslookout/newslookout.sh

CMD [ "su", "-", "newslookout", "-c", "/opt/newslookout/newslookout.sh" ]

# end of dockerfile #