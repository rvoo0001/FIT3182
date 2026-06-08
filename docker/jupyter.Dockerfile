# Extends the FIT3182 course PySpark + Jupyter image (the same one used to run
# the notebooks locally via `docker run ... fit3182/pyspark jupyter notebook`)
# with the extra Python packages the notebooks import that aren't bundled in it.
# It already ships Jupyter, PySpark (matching the Spark version the unit uses —
# see PYSPARK_SUBMIT_ARGS in 34080678_33590982_data_design_streaming.ipynb),
# pandas, numpy, matplotlib AND a vendored `kafka3` module (not a real PyPI
# package — installing it via pip fails, it's already present in site-packages).
# Multi-arch (amd64 + arm64), so it runs natively on Oracle Cloud's Ampere A1.
ARG BASE_IMAGE=fit3182/pyspark:latest
FROM ${BASE_IMAGE}

RUN pip install --no-cache-dir \
        pymongo \
        python-dotenv

EXPOSE 8888

# Shell form (not exec form) so $JUPYTER_TOKEN is expanded at container start —
# fixes the token to whatever's set in .env, instead of a new random one being
# generated (and the password UI's hash being lost) every time the container
# is recreated.
CMD jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser \
        --notebook-dir=/home/student/work \
        --NotebookApp.token="${JUPYTER_TOKEN}"
