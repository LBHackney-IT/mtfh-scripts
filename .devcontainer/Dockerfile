FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

RUN sudo apt update

COPY ./.devcontainer/scripts/ scripts/

RUN sh scripts/1_pre_requirements.sh

COPY requirements.txt .
RUN pip install -r requirements.txt

RUN pipx install ggshield

RUN sh scripts/2_post_requirements.sh
