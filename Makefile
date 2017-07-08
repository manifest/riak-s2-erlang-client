PROJECT = riaks2c
PROJECT_DESCRIPTION = Riak S2 Client

DEPS = \
	erlsom \
	jsx \
	gun

BUILD_DEPS = \
	version.mk

DEP_PLUGINS = \
	version.mk

dep_erlsom = git git://github.com/willemdj/erlsom.git ba84a5767fe6852af2b691fc0e234702d336d65f
dep_jsx = git git://github.com/talentdeficit/jsx.git v2.8.2
dep_gun = git git://github.com/manifest/gun.git feature/head-1xx
dep_version.mk = git git://github.com/manifest/version.mk.git master

TEST_DEPS = ct_helper
dep_ct_helper = git git://github.com/ninenines/ct_helper.git master

SHELL_DEPS = tddreloader
SHELL_OPTS = \
	-eval 'application:ensure_all_started($(PROJECT), permanent)' \
	-s tddreloader start

include erlang.mk

app:: rebar.config

GEN_RIAKS2_XSD = priv/schemas/riak-s2.xsd
GEN_RIAKS2_XSD_HRL_OUT = include/riaks2c_xsd.hrl
GEN_RIAKS2_XSD_SRC_OUT = src/riaks2c_xsd.erl
GEN_RIAKS2_XSD_SRC_MOD = $(basename $(notdir $(GEN_RIAKS2_XSD_SRC_OUT)) .erl)

gen:
	$(gen_verbose) erl \
		-noshell \
		-pa deps/erlsom/ebin \
		-eval "\
			ok = erlsom:write_xsd_hrl_file(\"$(GEN_RIAKS2_XSD)\", \"$(GEN_RIAKS2_XSD_HRL_OUT)\"), \
			{ok, Model} = erlsom:compile_xsd_file(\"$(GEN_RIAKS2_XSD)\", [{output_encoding, utf8}]), \
			XsdSrc = io_lib:format(\"%% SRC file generated by Makefile~n-module($(GEN_RIAKS2_XSD_SRC_MOD)).~n-export([scan/1,write/1,model/0]).~n~nscan(Xml) ->~ncase erlsom:scan(iolist_to_binary(Xml), model(), [{output_encoding, utf8}]) of {ok, Val, _} -> Val; {error, Reason} -> exit({bad_xml, Reason}) end.~n~nwrite(Struct) ->~ncase erlsom:write(Struct, model(), [{output, binary}]) of {ok, Xml} -> Xml; {error, Reason} -> exit({bad_xml_struct, Reason}) end.~n~nmodel() ->~n~p.\", [Model]), \
			ok = file:write_file(\"$(GEN_RIAKS2_XSD_SRC_OUT)\", XsdSrc), \
			erlang:halt(). \
		" \
	&& perl -pi -e 's/string\(\)/binary()/g' "$(GEN_RIAKS2_XSD_HRL_OUT)"

export DEVELOP_ENVIRONMENT = $(shell if [ -f .develop-environment ]; then cat .develop-environment; fi)
