FROM quay.io/astronomer/astro-runtime:11.3.0

USER root
#COPY ./dbt_project ./dbt_project
COPY --chown=astro:0 . .

USER astro
#RUN #python -m venv dbt_venv && \
#    . dbt_venv/bin/activate && \
#    pip install --no-cache-dir -r dbt_project/dbt-requirements.txt && \
#    source dbt_project/dbt.env && \
#    deactivate
