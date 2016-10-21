PROJECT = riaks2c
PROJECT_DESCRIPTION = Riak S2 Client
PROJECT_VERSION = 0.1.0

DEPS = \
	erlsom \
	gun

dep_erlsom = git git://github.com/willemdj/erlsom.git v1.4.1
dep_gun = git git://github.com/ninenines/gun.git 0414eede62946f995a5ab4879f9026548440199c

SHELL_DEPS = tddreloader
SHELL_OPTS = \
	-eval 'application:ensure_all_started($(PROJECT), permanent)' \
	-s tddreloader start

include erlang.mk
