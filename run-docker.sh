#!/bin/bash

PROJECT_DIR='/opt/sandbox/riaks2c-erlang-client'
DOCKER_ENV_CONFIG='.docker.env.config'
DOCKER_RUN_OPTIONS=${DOCKER_RUN_OPTIONS:-'-ti'}
DOCKER_CONTAINER_COMMAND=${DOCKER_CONTAINER_COMMAND:-'/bin/bash'}
DOCKER_CONTAINER_NAME='sandbox/riaks2c'

function CREATE_USER() {
	local EMAIL="${1}"
	local NAME="${2}"
	read -r RESULT <<-EOF
		curl -fsSL \
			-X POST 'http://localhost:8080/riak-cs/user' \
			-H 'Content-Type: application/json' \
			-d "{\"email\":\"${EMAIL}\",\"name\":\"${NAME}\"}"
	EOF
	echo "${RESULT}"
}

function DOCKER_BUILD_BEGIN() {
	local DOCKER_MACHINE_IP=$(docker-machine ip)
	local DOCKER_IP=${DOCKER_MACHINE_IP:-'localhost'}
	printf "{httpc_options, #{host => \"%s\", port => %s}}.\n" "${DOCKER_IP}" '8080' > "${DOCKER_ENV_CONFIG}"
}

read -r DOCKER_RUN_COMMAND <<-EOF
	function ADD_USER_TO_DEVCONF() { \
		local KEY="\${1}"; \
		local ID="\${2}"; \
		local SECRET="\${3}"; \
		local HOST="s3.amazonaws.com"; \
		local CONFIG_FILE="${PROJECT_DIR}/${DOCKER_ENV_CONFIG}"; \
		printf "{\${KEY}, #{id => <<\"%s\">>, secret => <<\"%s\">>, host => <<\"%s\">>}}.\n" "\${ID}" "\${SECRET}" "\${HOST}" >> "\${CONFIG_FILE}"; \
	} \
	&& service rsyslog start \
	&& riak start \
	&& riak-admin wait-for-service riak_kv \
	&& perl -pi -e 's/(?:(anonymous_user_creation = ).*)/\${1}on/' /etc/riak-cs/riak-cs.conf \
	&& stanchion start \
	&& riak-cs start \
	&& ADMIN=(\$($(CREATE_USER 'admin@example.org' 'admin') | jq -r '.key_id,.key_secret')) \
	&& \$(ADD_USER_TO_DEVCONF 'admin' "\${ADMIN[0]}" "\${ADMIN[1]}") \
	&& USER=(\$($(CREATE_USER 'user@example.org' 'user') | jq -r '.key_id,.key_secret')) \
	&& \$(ADD_USER_TO_DEVCONF 'user' "\${USER[0]}" "\${USER[1]}") \
	&& riak-cs stop \
	&& stanchion stop \
	&& perl -pi -e "s/admin\.key = .*/\
		admin.key = \${ADMIN[0]}\n\
		admin.secret = \${ADMIN[1]}\
		/" /etc/stanchion/stanchion.conf \
	&& perl -pi -e "s/admin\.key = .*/\
		admin.key = \${ADMIN[0]}\n\
		admin.secret = \${ADMIN[1]}\
		/" /etc/riak-cs/riak-cs.conf \
	&& perl -pi -e 's/(?:(anonymous_user_creation = ).*)/\${1}off/' /etc/riak-cs/riak-cs.conf \
	&& stanchion start \
	&& riak-cs start
EOF

DOCKER_BUILD_BEGIN
docker build -t ${DOCKER_CONTAINER_NAME} .
docker run ${DOCKER_RUN_OPTIONS} \
	-v $(pwd):${PROJECT_DIR} \
	-p 8080:8080 \
	${DOCKER_CONTAINER_NAME} \
	/bin/bash -c "set -x && cd ${PROJECT_DIR} && ${DOCKER_RUN_COMMAND} && set +x && ${DOCKER_CONTAINER_COMMAND}"
