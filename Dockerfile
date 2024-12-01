FROM quay.io/astronomer/astro-runtime:12.1.0

USER root
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN git clone https://github.com/sanchitvj/sports_betting_analytics_engine.git
WORKDIR /app/sports_betting_analytics_engine
RUN pip install .

#USER root
#COPY ./dbt_project ./dbt_project
#COPY --chown=astro:0 . .

#USER astro
# RUN python -m venv dbt_venv && \
#    . dbt_venv/bin/activate && \
#    pip install --no-cache-dir -r dbt_project/dbt-requirements.txt && \
#    source dbt_project/dbt.env && \
#    deactivate