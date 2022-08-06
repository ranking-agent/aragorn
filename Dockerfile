# leverage the renci python base image
FROM renciorg/renci-python-image:v0.0.1

# get some credit
LABEL maintainer="powen@renci.org"

# install basic tools
RUN apt-get update

# make a directory for the repo
RUN mkdir /repo

# go to the directory where we are going to upload the repo
WORKDIR /repo

# get the latest code
RUN git clone https://github.com/ranking-agent/aragorn.git

# go to the repo dir
WORKDIR /repo/aragorn

# install requirements
RUN pip install --upgrade pip
RUN cat requirements.txt
RUN pip install -r requirements.txt

# expose the default port
EXPOSE 4868

RUN chmod 777 -R .

USER nru

# start the service entry point
ENTRYPOINT ["bash", "main.sh"]