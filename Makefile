PROJECT = riaks2c
PROJECT_DESCRIPTION = Riak S2 Client
PROJECT_VERSION = 0.1.0

DEPS = \
	erlsom \
	gun

dep_erlsom = git git://github.com/willemdj/erlsom.git v1.4.1
dep_gun = git git://github.com/ninenines/gun.git 0414eede62946f995a5ab4879f9026548440199c

TEST_DEPS = ct_helper
dep_ct_helper = git git://github.com/ninenines/ct_helper.git master

SHELL_DEPS = tddreloader
SHELL_OPTS = \
	-eval 'application:ensure_all_started($(PROJECT), permanent)' \
	-s tddreloader start

include erlang.mk

GEN_RIAKS2_XSD = priv/schemas/riak-s2.xsd
GEN_RIAKS2_XSD_HRL_OUT = include/riaks2c_xsd.hrl
GEN_RIAKS2_XSD_SRC_OUT = src/riaks2c_xsd.erl
GEN_RIAKS2_XSD_SRC_MOD = $(basename $(notdir $(GEN_RIAKS2_XSD_SRC_OUT)) .erl)

gen:
	$(gen_verbose) erl \
		-noshell \
		-pa deps/erlsom/ebin \
		-eval '\
			ok = erlsom:write_xsd_hrl_file("$(GEN_RIAKS2_XSD)", "$(GEN_RIAKS2_XSD_HRL_OUT)"), \
			{ok, Model} = erlsom:compile_xsd_file("$(GEN_RIAKS2_XSD)", [{output_encoding, utf8}]), \
			XsdSrc = io_lib:format("%% SRC file generated by Makefile~n-module($(GEN_RIAKS2_XSD_SRC_MOD)).~n-export([model/0,scan/1]).~n~nscan(Input) ->~ncase erlsom:scan(Input, model(), [{output_encoding, utf8}]) of {ok, Val, _} -> Val; {error, Reason} -> exit({bad_xml, Reason}) end.~n~nmodel() ->~n~p.", [Model]), \
			ok = file:write_file("$(GEN_RIAKS2_XSD_SRC_OUT)", XsdSrc), \
			erlang:halt(). \
		' \
	&& perl -pi -e 's/string\(\)/binary()/g' "$(GEN_RIAKS2_XSD_HRL_OUT)"
