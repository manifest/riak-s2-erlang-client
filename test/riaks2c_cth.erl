-module(riaks2c_cth).

%% API
-export([
	init_config/0,
	gun_open/1,
	gun_down/1,
	make_bucket/0,
	make_key/0,
	make_content/0
]).

%% =============================================================================
%% API
%% =============================================================================

-spec init_config() -> list().
init_config() ->
	try
		{ok, S, _} = erl_scan:string(os:getenv("DEVELOP_ENVIRONMENT")),
		{ok, Conf} = erl_parse:parse_term(S),
		maps:fold(fun(Key, Val, Acc) -> [{Key, Val}|Acc] end, [], Conf)
	catch _:Reason -> error({missing_develop_environment, ?FUNCTION_NAME, Reason}) end.

-spec root_path(binary()) -> binary().
root_path(Path) ->
	Root = list_to_binary(filename:dirname(filename:join([filename:dirname(code:which(?MODULE))]))),
	<<Root/binary, $/, Path/binary>>.

-spec gun_open(list()) -> pid().
gun_open(Config) ->
	#{host := Host,
		port := Port} = ct_helper:config(s2_http, Config),
	{ok, Pid} = gun:open(Host, Port, #{retry => 0, protocols => [http]}),
	Pid.

-spec gun_down(pid()) -> ok.
gun_down(Pid) ->
	receive {gun_down, Pid, _, _, _, _} -> ok
	after 500 -> error(timeout) end.

%% A bucket name must obey the following rules, which produces a DNS-compliant bucket name:
%% - Must be from 3 to 63 characters.
%% - Must be one or more labels, each separated by a period (.). Each label:
%% - Must start with a lowercase letter or a number. Must end with a lowercase letter or a number. Can contain lowercase letters, numbers and dashes.
%% - Must not be formatted as an IP address (e.g., 192.168.9.2).
%% https://docs.basho.com/riak/cs/2.1.1/references/apis/storage/s3/put-bucket
-spec make_bucket() -> iodata().
make_bucket() ->
	Uniq = integer_to_binary(erlang:unique_integer([positive])),
	Size = rand:uniform(61) -byte_size(Uniq),
	[	<<(oneof(alphanum_lowercase_chars()))>>,
		Uniq,
		list_to_binary(vector(Size, [$-|alphanum_lowercase_chars()])),
		<<(oneof(alphanum_lowercase_chars()))>> ].

-spec make_key() -> iodata().
make_key() ->
	Uniq = integer_to_binary(erlang:unique_integer([positive])),
	Size = 255 -byte_size(Uniq),
	[Uniq, list_to_binary(vector(Size, alphanum_chars()))].

-spec make_content() -> {iodata(), iodata()}.
make_content() ->
	{crypto:strong_rand_bytes(1024), <<"application/octet-stream">>}.

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec oneof(list()) -> integer().
oneof(L) ->
	lists:nth(rand:uniform(length(L)), L).

-spec vector(non_neg_integer(), list()) -> list().
vector(MaxSize, L) ->
	vector(0, MaxSize, L, []).

-spec vector(non_neg_integer(), non_neg_integer(), list(), list()) -> list().
vector(Size, MaxSize, L, Acc) when Size < MaxSize ->
	vector(Size +1, MaxSize, L, [oneof(L)|Acc]);
vector(_, _, _, Acc) ->
	Acc.

-spec alphanum_lowercase_chars() -> list().
alphanum_lowercase_chars() ->
	"0123456789abcdefghijklmnopqrstuvwxyz".

-spec alphanum_chars() -> list().
alphanum_chars() ->
	"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".

