%% ----------------------------------------------------------------------------
%% The MIT License
%%
%% Copyright (c) 2016 Andrei Nesterov <ae.nesterov@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person obtaining a copy
%% of this software and associated documentation files (the "Software"), to
%% deal in the Software without restriction, including without limitation the
%% rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
%% sell copies of the Software, and to permit persons to whom the Software is
%% furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
%% IN THE SOFTWARE.
%% ----------------------------------------------------------------------------

-module(riaks2c_http).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	head/5,
	head/7,
	get/5,
	get/7,
	get/9,
	put/7,
	put/9,
	delete/7,
	await/4,
	await/5,
	await_head/5,
	await_body/4,
	fold_body/6,
	signature_v2/5,
	signature_v2/7,
	access_token_v2/2,
	throw_response_error/1,
	throw_response_error_404/1,
	return_response_error_404/1,
	default_request_timeout/0
]).

%% Definitions
-define(DEFAULT_REQUEST_TIMEOUT, 5000).

%% Types
-type qs()               :: [{binary(), binary() | true}].
-type headers()          :: [{binary(), iodata()}].
-type status()           :: 100..999.
-type fin()              :: fin | nofin.
-type head_handler()     :: fun((fin(), status(), headers()) -> any()).
-type body_handler()     :: fun((binary(), fin(), any()) -> any()).
-type response_handler() :: fun((status(), headers(), iodata()) -> iodata()).
-type request_options()  :: map().

-export_type([qs/0, headers/0, status/0, fin/0, head_handler/0, body_handler/0, response_handler/0, request_options/0]).

%% =============================================================================
%% API
%% =============================================================================

-spec head(pid(), iodata(), iodata(), iodata(), headers()) -> reference().
head(Pid, Id, Secret, Path, Headers) ->
	request(Pid, Id, Secret, <<"HEAD">>, Path, Headers).

-spec head(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> reference().
head(Pid, Id, Secret, Host, Path, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"HEAD">>, Host, Path, Bucket, Headers).

-spec get(pid(), iodata(), iodata(), iodata(), headers()) -> reference().
get(Pid, Id, Secret, Path, Headers) ->
	request(Pid, Id, Secret, <<"GET">>, Path, Headers).

-spec get(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> reference().
get(Pid, Id, Secret, Host, Path, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"GET">>, Host, Path, Bucket, Headers).

-spec get(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), qs(), iodata(), headers()) -> reference().
get(Pid, Id, Secret, Host, Path, SubRes, Qs, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"GET">>, Host, Path, SubRes, Qs, Bucket, Headers).

-spec put(pid(), iodata(), iodata(), iodata(), any(), iodata(), headers()) -> reference().
put(Pid, Id, Secret, Host, Path, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"PUT">>, Host, Path, Bucket, Headers).

-spec put(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), any(), iodata(), headers()) -> reference().
put(Pid, Id, Secret, Host, Path, Bucket, Val, ContentType, Headers0) ->
	Method = <<"PUT">>,
	Date = cow_date:rfc7231(erlang:universaltime()),
	ContentMD5 = base64:encode(erlang:md5(Val)),
	Sign = signature_v2(Secret, Method, [<<$/>>, Bucket, Path], ContentMD5, ContentType, Date, Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, Host]},
			{<<"content-md5">>, ContentMD5},
			{<<"content-type">>, ContentType},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	gun:request(Pid, Method, Path, Headers1, Val).

-spec delete(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> reference().
delete(Pid, Id, Secret, Host, Path, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"DELETE">>, Host, Path, Bucket, Headers).

-spec await(pid(), reference(), non_neg_integer(), response_handler()) -> any().
await(Pid, Ref, Timeout, Handle) ->
	Mref = monitor(process, Pid),
	await(Pid, Ref, Timeout, Mref, Handle).

-spec await(pid(), reference(), non_neg_integer(), reference(), response_handler()) -> any().
await(Pid, Ref, Timeout, Mref, Handle) ->
	await_head(
		Pid, Ref, Timeout, Mref,
		fun
			(nofin, Status, Headers) ->
				Data = await_body(Pid, Ref, Timeout, Mref),
				Handle(Status, Headers, Data);
			(fin, Status, Headers) ->
				demonitor(Mref, [flush]),
				gun:flush(Ref),
				Handle(Status, Headers, <<>>)
		end).

%% Be careful, you must always flush stream messages and demonitor the stream proccess
%% in the 'Handle :: head_handler()' function. The function must not fail.
%% See riaks2c_http.erl:await/5 as an example.
-spec await_head(pid(), reference(), non_neg_integer(), reference(), head_handler()) -> any().
await_head(Pid, Ref, Timeout, Mref, Handle) ->
	receive
		{gun_response, Pid, Ref, IsFin, Status, Headers} ->
			Handle(IsFin, Status, Headers);
		{gun_error, Pid, Ref, Reason} ->
			demonitor(Mref, [flush]),
			gun:flush(Ref),
			exit(Reason);
		{gun_error, Pid, Reason} ->
			demonitor(Mref, [flush]),
			gun:flush(Ref),
			exit(Reason);
		{'DOWN', Mref, process, Pid, Reason} ->
			gun:flush(Ref),
			exit(Reason)
	after Timeout ->
		demonitor(Mref, [flush]),
		gun:flush(Ref),
		exit(timeout)
	end.

-spec await_body(pid(), reference(), non_neg_integer(), reference()) -> iodata().
await_body(Pid, Ref, Timeout, Mref) ->
	fold_body(Pid, Ref, Timeout, Mref, <<>>, fun accumulate_body/3).

%% Be careful, 'Handle :: body_handler()' function must not fail.
-spec fold_body(pid(), reference(), non_neg_integer(), reference(), iodata(), body_handler()) -> iodata().
fold_body(Pid, Ref, Timeout, Mref, Acc, Handle) ->
	receive
		{gun_data, Pid, Ref, nofin =IsFin, Data} ->
			fold_body(Pid, Ref, Timeout, Mref, Handle(Data, IsFin, Acc), Handle);
		{gun_data, Pid, Ref, fin =IsFin, Data} ->
			demonitor(Mref, [flush]),
			gun:flush(Ref),
			Handle(Data, IsFin, Acc);
		{gun_error, Pid, Ref, Reason} ->
			demonitor(Mref, [flush]),
			gun:flush(Ref),
			exit(Reason);
		{gun_error, Pid, Reason} ->
			demonitor(Mref, [flush]),
			gun:flush(Ref),
			exit(Reason);
		{'DOWN', Mref, process, Pid, Reason} ->
			gun:flush(Ref),
			exit(Reason)
	after Timeout ->
		demonitor(Mref, [flush]),
		gun:flush(Ref),
		exit(timeout)
	end.

-spec signature_v2(iodata(), iodata(), iodata(), iodata(), headers()) -> iodata().
signature_v2(Secret, Method, Resource, Date, Headers) ->
	signature_v2(Secret, Method, Resource, <<>>, <<>>, Date, Headers).

-spec signature_v2(iodata(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> iodata().
signature_v2(Secret, Method, Resource, ContentMD5, ContentType, Date, Headers) ->
	Input =
		[	Method, <<$\n>>,
			ContentMD5, <<$\n>>,
			ContentType, <<$\n>>,
			Date, <<$\n>>,
			amz_headers(Headers),
			Resource ],
	base64:encode(crypto:hmac(sha, Secret, Input)).

-spec access_token_v2(iodata(), iodata()) -> iodata().
access_token_v2(Id, Sign) ->
	[<<"AWS ">>, Id, <<$:>>, Sign].

-spec throw_response_error(iodata()) -> no_return().
throw_response_error(Xml) ->
	exit(riaks2c_xsd:scan(Xml)).

-spec throw_response_error_404(iodata()) -> no_return().
throw_response_error_404(<<>>) ->
	error(unknown);
throw_response_error_404(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchKey">>          -> {Bucket, Key} = parse_resource_key(R), error({bad_key, Bucket, Key});
		<<"NoSuchBucket">>       -> error({bad_bucket, parse_resource_bucket(R)});
		<<"NoSuchBucketPolicy">> -> error({bad_bucket_policy, parse_resource_bucket(R)})
	end.

-spec return_response_error_404(iodata()) -> {error, any()}.
return_response_error_404(<<>>) ->
	{error, unknown};
return_response_error_404(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchKey">>          -> {Bucket, Key} = parse_resource_key(R), {error, {bad_key, Bucket, Key}};
		<<"NoSuchBucket">>       -> {error, {bad_bucket, parse_resource_bucket(R)}};
		<<"NoSuchBucketPolicy">> -> {error, {bad_bucket_policy, parse_resource_bucket(R)}}
	end.

-spec default_request_timeout() -> non_neg_integer().
default_request_timeout() ->
	5000.

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec request(pid(), iodata(), iodata(), binary(), iodata(), headers()) -> reference().
request(Pid, Id, Secret, Method, Path, Headers0) ->
	Date = cow_date:rfc7231(erlang:universaltime()),
	Sign = signature_v2(Secret, Method, Path, Date, Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	gun:request(Pid, Method, Path, Headers1).

-spec request(pid(), iodata(), iodata(), binary(), iodata(), iodata(), iodata(), headers()) -> reference().
request(Pid, Id, Secret, Method, Host, Path, Bucket, Headers0) ->
	Date = cow_date:rfc7231(erlang:universaltime()),
	Sign = signature_v2(Secret, Method, [<<$/>>, Bucket, Path], Date, Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, Host]},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	gun:request(Pid, Method, Path, Headers1).

%% This `request` function is used for requests that contain query string parameters,
%% with or without subresource (empty binary `<<>>` is passed in that case).
%% The problem is that subresource is also represented as query string parameter.
%% While only subresource is used for building request's signature, we need a way
%% to distinguish it from other parameters.
%%
%% For all other cases we use other `request` functions, passing subresource
%% withing `Path` argument if needed.
-spec request(pid(), iodata(), iodata(), binary(), iodata(), iodata(), iodata(), qs(), iodata(), headers()) -> reference().
request(Pid, Id, Secret, Method, Host, Path, SubRes, Qs, Bucket, Headers0) ->
	Date = cow_date:rfc7231(erlang:universaltime()),
	Sign = signature_v2(Secret, Method, resource([<<$/>>, Bucket, Path], SubRes), Date, Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, Host]},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	gun:request(Pid, Method, path(Path, SubRes, Qs), Headers1).

-spec path(iodata(), iodata(), qs()) -> iodata().
path(Path, <<>>, [])   -> Path;
path(Path, <<>>, Qs)   -> [Path, <<$?>>, cow_qs:qs(Qs)];
path(Path, SubRes, []) -> [Path, <<$?>>, SubRes];
path(Path, SubRes, Qs) -> [Path, <<$?>>, SubRes, <<$&>>, cow_qs:qs(Qs)].

-spec resource(iodata(), iodata()) -> iodata().
resource(Path, <<>>)   -> Path;
resource(Path, SubRes) -> [Path, <<$?>>, SubRes].

-spec amz_headers(list()) -> list().
amz_headers(Input) ->
	amz_headers(Input, ordsets:new()).

-spec amz_headers(list(), list()) -> list().
amz_headers([{<<"x-amz-", _/bits>> =Key, Val}|T], L) ->
	amz_headers(T, ordsets:add_element([Key, <<$:>>, Val, <<$\n>>], L));
amz_headers([_|T], L) ->
	amz_headers(T, L);
amz_headers([], L) ->
	L.

-spec parse_resource_bucket(binary()) -> binary().
parse_resource_bucket(<<$/, Rest/bits>>) ->
	parse_resource_bucket(Rest, <<>>).

-spec parse_resource_bucket(binary(), binary()) -> binary().
parse_resource_bucket(<<$/, _/bits>>, Acc)   -> Acc;
parse_resource_bucket(<<C, Rest/bits>>, Acc) -> parse_resource_bucket(Rest, <<Acc/binary, C>>).

-spec parse_resource_key(binary()) -> {binary(), binary()}.
parse_resource_key(<<$/, Rest/bits>>) ->
	parse_resource_key(Rest, <<>>).

-spec parse_resource_key(binary(), binary()) -> {binary(), binary()}.
parse_resource_key(<<$/, Rest/bits>>, Acc) -> {Acc, Rest};
parse_resource_key(<<C, Rest/bits>>, Acc)  -> parse_resource_key(Rest, <<Acc/binary, C>>).

-spec accumulate_body(binary(), fin(), binary()) -> binary().
accumulate_body(Data, _IsFin, Acc) ->
	<<Acc/binary, Data/binary>>.
