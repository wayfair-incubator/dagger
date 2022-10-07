ARG BASE_OS=${BASE_OS:-"BASE_OS NOT SET"}
ARG DOCKER_PYTHON_VERSION=${DOCKER_PYTHON_VERSION:-"DOCKER_PYTHON_VERSION NOT SET"}
ARG PYTHON_VERSION=${PYTHON_VERSION:-"PYTHON_VERSION NOT SET"}
ARG SERVICE_RELEASE_DATE=${SERVICE_RELEASE_DATE:-"SERVICE_RELEASE_DATE NOT SET"}

FROM wayfair/python/${BASE_OS}-${DOCKER_PYTHON_VERSION}-devbox:${PYTHON_VERSION}-${SERVICE_RELEASE_DATE}

ARG PROJECT_NAME=${PROJECT_NAME:-"PROJECT_NAME NOT SET"}
ARG PROJECT_ROOT_IMPORT_NAME=${PROJECT_ROOT_IMPORT_NAME:-"PROJECT_ROOT_IMPORT_NAME NOT SET"}
ARG PROJECT_LABEL=${PROJECT_LABEL:-"PROJECT_LABEL NOT SET"}
ARG PROJECT_MAINTAINER=${PROJECT_MAINTAINER:-"PROJECT_MAINTAINER NOT SET"}
ARG WF_VERSION

ENV wf_name ${PROJECT_NAME}
ENV PROJECT_ROOT_IMPORT_NAME ${PROJECT_ROOT_IMPORT_NAME}
ENV wf_label ${PROJECT_LABEL}
ENV wf_maintainer ${PROJECT_MAINTAINER}s

# Add image metadata for service image - a requirement for kubernetes deployments
LABEL \
  com.wayfair.app=${wf_name} \
  com.wayfair.description=${wf_label} \
  com.wayfair.maintainer=${wf_maintainer} \
  com.wayfair.vendor="Wayfair LLC." \
  com.wayfair.version=${WF_VERSION}

# 'root' user should be used to install system dependencies, and other things that require elevated permissions
USER root

RUN yum -y install snappy lz4

EXPOSE 6066
