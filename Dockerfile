FROM public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

#TODO add your project code and dependencies to the image
# # -------------
# # version 1: doesn't work because 
# # -------------
# RUN pip install uv
# COPY pyproject.toml .
# COPY README.md .
# RUN uv venv
# RUN source .venv/bin/activate
# RUN uv sync
# COPY src/ src/
# RUN uv pip install -e .
# CMD ["uv", "run", "python3", "-m", "capstonellm.tasks.clean"]

# # -------------
# # version 2
# # -------------
# RUN pip install uv

# COPY pyproject.toml .
# COPY uv.lock .
# RUN uv sync --no-install-project

# COPY src/ src/
# RUN uv sync

# # RUN uv pip install -e .

# CMD ["uv", "run", "python3", "-m", "capstonellm.tasks.clean"]

# -------------
# version 3
# -------------
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .
RUN pip3 install .

# CMD ["uv", "run", "python3", "-m", "capstonellm.tasks.clean"]

