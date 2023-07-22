FROM nvidia/cuda:11.7.1-base-ubuntu22.04

ENV SHELL=/bin/bash
WORKDIR /app

# Build with some basic utilities
RUN apt-get update && apt-get install -y \
    python3-pip \
    apt-utils \
    git

# alias python='python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /thesis
COPY . .
RUN pip install --upgrade pip
RUN pip install --upgrade -r requirements.txt

RUN git clone https://github.com/bradhackinen/nama.git
WORKDIR /thesis/nama
RUN pip install -r requirements.txt
RUN pip install ./nama; exit 0
RUN pip install .

WORKDIR /thesis
EXPOSE 8888
ENTRYPOINT ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--allow-root", "--no-browser"]
