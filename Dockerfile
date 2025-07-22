# leverage the renci python base image
FROM ghcr.io/translatorsri/renci-python-image:3.11.5

#Set the branch
ARG BRANCH_NAME=main

# install basic tools
RUN apt-get update

# make a directory for the repo
RUN mkdir /repo

#This is for reasoner-pydantic
ENV PYTHONHASHSEED=0

# go to the directory where we are going to upload the repo
WORKDIR /repo

# get the latest code
RUN git clone --branch $BRANCH_NAME --single-branch https://github.com/ranking-agent/aragorn.git

# go to the repo dir
WORKDIR /repo/aragorn

# install requirements
RUN pip install -r requirements.txt

# expose the default port
EXPOSE 4868

RUN chmod 777 -R .

USER nru

# start the service entry point
ENTRYPOINT ["bash", "main.sh"]