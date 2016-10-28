-module(riaks2c_test).

%% API
-export([
	init_config/0,
	gun_open/1,
	gun_down/1
]).

%% =============================================================================
%% API
%% =============================================================================

-spec init_config() -> list().
init_config() ->
	{ok, Config} = file:consult(root_path(<<".docker.env.config">>)),
	Config.

-spec root_path(binary()) -> binary().
root_path(Path) ->
	Root = list_to_binary(filename:dirname(filename:join([filename:dirname(code:which(?MODULE))]))),
	<<Root/binary, $/, Path/binary>>.

-spec gun_open(list()) -> pid().
gun_open(Config) ->
	#{host := Host,
		port := Port} = ct_helper:config(httpc_options, Config),
	{ok, Pid} = gun:open(Host, Port, #{retry => 0, protocols => [http]}),
	Pid.

-spec gun_down(pid()) -> ok.
gun_down(Pid) ->
	receive {gun_down, Pid, _, _, _, _} -> ok
	after 500 -> error(timeout) end.

