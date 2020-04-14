FROM ubuntu:19.04

RUN useradd -ms /bin/bash consolidator
ENV AIRFLOW_HOME /usr/local/airflow
WORKDIR /home/consolidator/opt/parquet-consolidator

COPY requirements.txt .

RUN apt-get update -y && apt-get install -y \
	wget \
	build-essential \
    vim \
	openssh-server \
	openssl \
	net-tools \
	git \
	locales \
	sudo \
	dumb-init \
	curl \
	bsdtar \
  openssh-server \
  python3-pip python3-dev python3-setuptools \
	--no-install-recommends \
	&& rm -rf /var/lib/apt/lists/* \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

USER consolidator
RUN mkdir /home/consolidator/.aws
RUN pip install -r requirements.txt --user

# COPY config config
COPY src src
USER consolidator
COPY --chown=consolidator:consolidator credentials /home/consolidator/.aws/credentials

ENTRYPOINT []
# CMD ["python"]
CMD ["python /home/consolidator/opt/parquet-consolidator/src/parquet-consolidator.py --s3file ercot_nodal_system_parameters"]