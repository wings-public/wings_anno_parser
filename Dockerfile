FROM node:latest
RUN apt-get update && apt-get -y install gzip \
    unzip \
    vim
WORKDIR /repo
COPY . .
RUN npm install
WORKDIR /repo/src/parser
ENV PARSE_LOG /dataFiles/parser
