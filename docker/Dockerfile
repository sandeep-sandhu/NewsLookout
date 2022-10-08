# Dockerfile for NewsLookout web scraping application.

FROM python:latest

WORKDIR /opt

RUN mkdir -p /var/log/newslookout \
    && mkdir -p /var/cache/newslookout_data \
    && mkdir -p /var/run/newslookout \
    && mkdir -p /etc/newslookout \
    && mkdir -p /opt/newslookout/models/nltk \
    && mkdir -p /opt/newslookout/models/bert_models

ENV NLTK_DATA="/opt/newslookout/models/nltk"
ENV NEWSLOOKOUT_DATA="/var/cache/newslookout_data"
ENV NEWSLOOKOUT_HOME="/opt/newslookout"
ENV NEWSLOOKOUT_CONFIG="/etc/newslookout/newslookout.conf"

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# install the models for natural language processing:
RUN python3 -m spacy download en_core_web_lg \
    && python3 -c "import nltk; nltk.download('punkt'); nltk.download('maxent_treebank_pos_tagger'); nltk.download('reuters'); nltk.download('universal_treebanks_v20')"

# copy finbert model weights
COPY models/finbert /opt/newslookout/models/bert_models

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

RUN useradd -ms /bin/bash newslookout

# Assumes the latest NewsLookout release has been downloaded
# from the Github release page to the local directory:
COPY newslookout /opt/newslookout

COPY newslookout.sh /opt/

COPY docker/newslookout_docker.conf $NEWSLOOKOUT_CONFIG

CMD [ "bash", "/opt/newslookout.sh" ]


# end of dockerfile #