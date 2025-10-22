FROM quay.io/jupyter/base-notebook:python-3.11

USER root
ENV DEBIAN_FRONTEND=noninteractive

RUN rm /usr/local/bin/before-notebook.d/10activate-conda-env.sh

# Stop conda from automatically activating the base environment in new shells
RUN conda config --set auto_activate_base false

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    python3-venv \
    openssh-server \
    curl \
    git \
    vim \
    ca-certificates \
    rsync && \
    rm -rf /var/lib/apt/lists/*

# Install Google Chrome directly using wget and dpkg to bypass all apt key issues
RUN apt-get update && apt-get install -y \
    wget \
    --no-install-recommends && \
    wget -O /tmp/google-chrome-stable_current_amd64.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    dpkg -i /tmp/google-chrome-stable_current_amd64.deb || apt-get -f install -y && \
    rm -f /tmp/google-chrome-stable_current_amd64.deb && \
    rm -rf /var/lib/apt/lists/*



# Set up user environment
ENV HOME_DIR=/home/${NB_USER}

ENV CODE_DIR=/opt/code

WORKDIR ${CODE_DIR}
RUN chown -R ${NB_UID}:${NB_UID} ${CODE_DIR}
COPY --chown=${NB_UID}:${NB_GID} . ${CODE_DIR}/mainsequence-sdk

# Create a new virtual environment that is separate from conda
RUN python -m venv /opt/venv

RUN mkdir /opt/kernel

# Make sure 'jovyan' (NB_USER) can write to /opt/venv
RUN chown -R ${NB_UID}:${NB_GID} /opt/venv
RUN chown -R ${NB_UID}:${NB_GID} /opt/kernel

# Put our venv first on PATH for both build and runtime
ENV PATH="/opt/venv/bin:$PATH"

# Switch to the notebook user so pip installs go into /opt/venv for jovyan
USER ${NB_USER}
WORKDIR ${HOME_DIR}

RUN echo 'export PATH="/opt/venv/bin:$PATH"' >> ~/.bashrc

# Setup jupyterlab
RUN pip install --no-cache-dir ipykernel && \
    python -m ipykernel install \
      --prefix=/opt/kernel \
      --name my-venv \
      --display-name "Python 3 (my-venv)"

RUN pip install --no-cache-dir \
    streamlit \
    jupyter-server-proxy

COPY scripts/jupyter_server_config.py /opt/venv/etc/jupyter/jupyter_server_config.py


RUN pip install --no-cache-dir jupyterhub
RUN pip install --no-cache-dir jupyterlab
RUN pip install --no-cache-dir ipywidgets

# Now install your requirements purely with pip in this venv
RUN pip install --no-cache-dir mainsequence

# Create requirements.txt, excluding mainsequence to avoid the commit hash, then add it back without the hash to always use latest
RUN pip freeze > ${CODE_DIR}/mainsequence-sdk/requirements.txt

# Make scripts executable, if you need them
RUN chmod +x ${CODE_DIR}/mainsequence-sdk/scripts/get_git_and_run.sh
RUN chmod +x ${CODE_DIR}/mainsequence-sdk/scripts/setup_project.sh

# Provide any environment variables you need
ENV TDAG_CONFIG_PATH=${HOME_DIR}/tdag/default_config.yml \
    TDAG_RAY_CLUSTER_ADDRESS=ray://localhost:10001 \
    TDAG_RAY_API_ADDRESS=http://localhost:8265 \
    TDAG_RAY_SERVE_HOST=0.0.0.0 \
    TDAG_RAY_SERVE_PORT=8003 \
    MLFLOW_ENDPOINT=http://localhost:5000