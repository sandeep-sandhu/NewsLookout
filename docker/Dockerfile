
# Dockerfile for NewsLookout web scraping application.

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

USER root

WORKDIR /tmp

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENTRYPOINT ["tini", "-g", "--"]

# set up environment
ENV PATH home/newslookout/app_base:$PATH
CMD ["/usr/bin/su", "-", "newslookout", "-c", "home/newslookout/app_base/newslookout.sh > /var/log/newslookout/stdout "]


ENV GITHUB_RELEASE_URL=https://github.com/sandeep-sandhu/NewsLookout/releases/latest
ENV NLTK_DATA="/var/cache/newslookout/nltk"

# Assumes the latest NewsLookout release has been downloaded
# from the Github release page to the local directory:
ENV APP_RELEASE_DIR=NewsLookout-2.0.0
COPY NewsLookout-2.0.0.tar.gz /tmp/$APP_RELEASE_DIR.tar.gz

RUN apt-get -qq update \
    && apt -y upgrade \
	&& apt-get -y install curl bzip2 p7zip xz-utils gzip software-properties-common dirmngr zip rsync ssh sudo net-tools tini ca-certificates locales \
	&& echo "en_US.UTF-8 UTF-8" > /etc/locale.gen  \
    && locale-gen \
    && update-ca-certificates \
	&& useradd -ms /bin/bash newslookout \
    && apt-get -y install python3-pip \
    && tar -xvzf /tmp/$APP_RELEASE_DIR.tar.gz --directory=/home/newslookout \
    && mv -v /home/newslookout/$APP_RELEASE_DIR /home/newslookout/app_base \
    && rm /tmp/$APP_RELEASE_DIR.tar.gz \
    && mkdir -p /var/cache/newslookout \
    && mkdir -p /var/cache/newslookout/nltk \
    && cp -v /home/newslookout/app_base/data/completed_urls.db /var/cache/newslookout \
    && chown -cR newslookout:newslookout /var/cache/newslookout \
    && mkdir -p /var/log/newslookout \
    && touch /var/log/newslookout/newslookout.log \
    && chown -cR newslookout:newslookout /var/log/newslookout \
    && mkdir -p /var/run/newslookout \
    && chown -cR newslookout:newslookout /var/run/newslookout \
    && echo "#!/bin/bash" > /home/newslookout/app_base/newslookout.sh \
    && echo "python3 -c \"newslookout -c home/newslookout/app_base/conf/newslookout.conf -d `date +%Y-%m-%d` \"" >> /home/newslookout/app_base/newslookout.sh \
    && chmod +x /home/newslookout/app_base/newslookout.sh \
    && chown -cR newslookout:newslookout /home/newslookout \
    && apt-get clean \
    && apt-get -qq -y autoremove

# install the models for natural language processing:
RUN su - newslookout -c "pip3 --no-cache-dir install torch --extra-index-url https://download.pytorch.org/whl/cpu" \
    && su - newslookout -c "pip3 --no-cache-dir install -r home/newslookout/app_base/requirements.txt" \
    && su - newslookout -c "python3 -m spacy download en_core_web_lg" \
    && su - newslookout -c "python3 -c \"import nltk; nltk.download('punkt'); nltk.download('maxent_treebank_pos_tagger'); nltk.download('reuters'); nltk.download('universal_treebanks_v20')\"" \
    && apt-get clean \
    && apt-get -qq -y autoremove \
    && rm -rf /var/lib/apt/lists/* /var/apt/cache/* /var/log/dpkg.log


LABEL \
    maintainer="sandeep.sandhu@gmx.com" \
    org.label-schema.build-date=$BUILD_DATE \
    org.label-schema.docker.dockerfile="/Dockerfile" \
    org.label-schema.license="Apache License 2.0" \
    org.label-schema.name="sandeep-sandhu/newslookout" \
    org.label-schema.version="$APP_RELEASE_DIR" \
    org.label-schema.url="https://github.com/sandeep-sandhu" \
    org.label-schema.vcs-type="Git"


# end of dockerfile #