FROM quay.io/astronomer/astro-runtime:12.1.0

USER root
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
#COPY .env /app/.env # no need to copy this one
COPY my.env /app/my.env

RUN git clone https://github.com/sanchitvj/sports_betting_analytics_engine.git
WORKDIR /app/sports_betting_analytics_engine
#RUN git checkout dev
RUN pip install .

RUN pip install --upgrade awscli
