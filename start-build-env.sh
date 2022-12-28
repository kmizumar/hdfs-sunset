#!/usr/bin/env bash

set -e		# exit on error

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Set the home directory in the Docker contianer.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

docker build -t "hdfs-sunset-${USER_ID}-sshd" - <<UserSpecificDocker
FROM hadoop-build-${USER_ID}-sshd
ARG SCALA_VERSION=2.11.12
ARG SPARK_VERSION=2.4.7
ENV SDKMAN_DIR /home/${USER_NAME}/.sdkman
SHELL ["/bin/bash", "-c"]
RUN apt-get update && apt-get install -y ca-certificates curl git less openssh-client openssl \
  tree unzip vim-tiny zip
ENV NOTVISIBLE "in users profile"
EXPOSE 22223
USER ${USER_NAME}
RUN curl -s "https://get.sdkman.io" | bash \
  && source "\${SDKMAN_DIR}/bin/sdkman-init.sh" \
  && { \
       echo ; \
       echo '#THIS MUST BE AT THE END OF THE FILE FOR SDKMAN TO WORK!!!'; \
       echo 'export SDKMAN_DIR="\${SDKMAN_DIR}"'; \
       echo '[[ -s "\${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "\${SDKMAN_DIR}/bin/sdkman-init.sh"'; \
     } >> /home/${USER_NAME}/.bashrc \
  && { \
       echo ; \
       echo '#THIS MUST BE AT THE END OF THE FILE FOR SDKMAN TO WORK!!!'; \
       echo 'export SDKMAN_DIR="\${SDKMAN_DIR}"'; \
       echo '[[ -s "\${SDKMAN_DIR}/bin/sdkman-init.sh" ]] && source "\${SDKMAN_DIR}/bin/sdkman-init.sh"'; \
     } >> /home/${USER_NAME}/.zshrc \
  && sdk install java \$(sdk list java | grep -o "8\.[0-9]*\.[0-9]*\-open" | head -1) \
  && sdk install sbt \
  && sdk install scala \${SCALA_VERSION} \
  && sdk install spark \${SPARK_VERSION}

UserSpecificDocker



#If this env varible is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN=${DOCKER_INTERACTIVE_RUN-"-i -t"}

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  --network host \
  -v "${PWD}:${DOCKER_HOME_DIR}/hdfs-sunset${V_OPTS:-}" \
  -w "${DOCKER_HOME_DIR}/hdfs-sunset${V_OPTS:-}" \
  -v "${HOME}/.m2:${DOCKER_HOME_DIR}/.m2${V_OPTS:-}" \
  -v "${HOME}/.gnupg:${DOCKER_HOME_DIR}/.gnupg${V_OPTS:-}" \
  -v "${HOME}/.ssh:${DOCKER_HOME_DIR}/.ssh${V_OPTS:-}" \
  -v "${HOME}/container-dot-cache:${DOCKER_HOME_DIR}/.cache${V_OPTS:-}" \
  -u "${USER_ID}" \
  "hdfs-sunset-${USER_ID}-sshd" "$@"
