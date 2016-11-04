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
	head/7,
	head/9,
	get/7,
	get/9,
	put/9,
	put/11,
	delete/9,
	signature_v2/5,
	signature_v2/7,
	access_token_v2/2,
	throw_response_error/1,
	throw_response_error_404/2,
	throw_response_error_404/3,
	return_response_error_404/2,
	return_response_error_404/3
]).

%% Definitions
-define(DEFAULT_REQUEST_TIMEOUT, 5000).

%% Types
-type request_options()  :: map().
-type response_handler() :: fun((100..999, cow_http:headers(), iodata()) -> iodata()).

-export_type([request_options/0, response_handler/0]).

%% =============================================================================
%% API
%% =============================================================================

-spec head(pid(), iodata(), iodata(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
head(Pid, Id, Secret, Path, Headers0, Opts, Handle) ->
	request(Pid, Id, Secret, <<"HEAD">>, Path, Headers0, Opts, Handle).

-spec head(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
head(Pid, Id, Secret, Host, Path, Bucket, Headers0, Opts, Handle) ->
	request(Pid, Id, Secret, <<"HEAD">>, Host, Path, Bucket, Headers0, Opts, Handle).

-spec get(pid(), iodata(), iodata(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
get(Pid, Id, Secret, Path, Headers0, Opts, Handle) ->
	request(Pid, Id, Secret, <<"GET">>, Path, Headers0, Opts, Handle).

-spec get(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
get(Pid, Id, Secret, Host, Path, Bucket, Headers0, Opts, Handle) ->
	request(Pid, Id, Secret, <<"GET">>, Host, Path, Bucket, Headers0, Opts, Handle).

-spec put(pid(), iodata(), iodata(), iodata(), any(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
put(Pid, Id, Secret, Host, Path, Bucket, Headers, Opts, Handle) ->
	request(Pid, Id, Secret, <<"PUT">>, Host, Path, Bucket, Headers, Opts, Handle).

-spec put(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), any(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
put(Pid, Id, Secret, Host, Path, Bucket, Val, ContentType, Headers0, Opts, Handle) ->
	Timeout = maps:get(requesttimeout, Opts, ?DEFAULT_REQUEST_TIMEOUT),
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
	handle_response(Pid, gun:request(Pid, Method, Path, Headers1, Val), Timeout, Handle).

-spec delete(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
delete(Pid, Id, Secret, Host, Path, Bucket, Headers0, Opts, Handle) ->
	request(Pid, Id, Secret, <<"DELETE">>, Host, Path, Bucket, Headers0, Opts, Handle).

-spec signature_v2(iodata(), iodata(), iodata(), iodata(), cow_http:headers()) -> iodata().
signature_v2(Secret, Method, Resource, Date, Headers) ->
	signature_v2(Secret, Method, Resource, <<>>, <<>>, Date, Headers).

-spec signature_v2(iodata(), iodata(), iodata(), iodata(), iodata(), iodata(), cow_http:headers()) -> iodata().
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

-spec throw_response_error_404(iodata(), iodata()) -> no_return().
throw_response_error_404(<<>>, Bucket) ->
	error({bad_bucket, Bucket});
throw_response_error_404(Xml, Bucket) ->
	#'Error'{'Code' = Code} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchBucket">>       -> error({bad_bucket, Bucket});
		<<"NoSuchBucketPolicy">> -> error({bad_bucket_policy, Bucket})
	end.

-spec throw_response_error_404(iodata(), iodata(), iodata()) -> no_return().
throw_response_error_404(<<>>, Bucket, Key) ->
	error({bad_key, Bucket, Key});
throw_response_error_404(Xml, Bucket, Key) ->
	#'Error'{'Code' = Code} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchKey">>          -> error({bad_key, Bucket, Key});
		<<"NoSuchBucket">>       -> error({bad_bucket, Bucket});
		<<"NoSuchBucketPolicy">> -> error({bad_bucket_policy, Bucket})
	end.

-spec return_response_error_404(iodata(), iodata()) -> {error, any()}.
return_response_error_404(<<>>, Bucket) ->
	{error, {bad_bucket, Bucket}};
return_response_error_404(Xml, Bucket) ->
	#'Error'{'Code' = Code} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchBucket">>       -> {error, {bad_bucket, Bucket}};
		<<"NoSuchBucketPolicy">> -> {error, {bad_bucket_policy, Bucket}}
	end.

-spec return_response_error_404(iodata(), iodata(), iodata()) -> {error, any()}.
return_response_error_404(<<>>, Bucket, Key) ->
	{error, {bad_key, Bucket, Key}};
return_response_error_404(Xml, Bucket, Key) ->
	#'Error'{'Code' = Code} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchKey">>          -> {error, {bad_key, Bucket, Key}};
		<<"NoSuchBucket">>       -> {error, {bad_bucket, Bucket}};
		<<"NoSuchBucketPolicy">> -> {error, {bad_bucket_policy, Bucket}}
	end.

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec request(pid(), iodata(), iodata(), binary(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
request(Pid, Id, Secret, Method, Path, Headers0, Opts, Handle) ->
	Timeout = maps:get(requesttimeout, Opts, ?DEFAULT_REQUEST_TIMEOUT),
	Date = cow_date:rfc7231(erlang:universaltime()),
	Sign = signature_v2(Secret, Method, Path, Date, Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	handle_response(Pid, gun:request(Pid, Method, Path, Headers1), Timeout, Handle).

-spec request(pid(), iodata(), iodata(), binary(), iodata(), iodata(), iodata(), cow_http:headers(), request_options(), response_handler()) -> any().
request(Pid, Id, Secret, Method, Host, Path, Bucket, Headers0, Opts, Handle) ->
	Timeout = maps:get(requesttimeout, Opts, ?DEFAULT_REQUEST_TIMEOUT),
	Date = cow_date:rfc7231(erlang:universaltime()),
	Sign = signature_v2(Secret, Method, [<<$/>>, Bucket, Path], Date, Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, Host]},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	handle_response(Pid, gun:request(Pid, Method, Path, Headers1), Timeout, Handle).

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

-spec handle_response(pid(), reference(), non_neg_integer(), response_handler()) -> any().
handle_response(Pid, Ref, Timeout, Handle) ->
	Mref = monitor(process, Pid),
	receive
		{gun_response, Pid, Ref, nofin, Status, Headers} ->
			Data = await_data(Pid, Ref, Timeout, Mref, <<>>),
			demonitor(Mref, [flush]),
			Handle(Status, Headers, Data);
		{gun_response, Pid, Ref, fin, Status, Headers} ->
			demonitor(Mref, [flush]),
			Handle(Status, Headers, <<>>);
		{gun_error, Pid, Ref, Reason} ->
			demonitor(Mref, [flush]),
			exit(Reason);
		{gun_error, Pid, Reason} ->
			demonitor(Mref, [flush]),
			exit(Reason);
		{'DOWN', Mref, process, Pid, Reason} ->
			exit(Reason)
	after Timeout ->
		demonitor(Mref, [flush]),
		exit(timeout)
	end.

-spec await_data(pid(), reference(), non_neg_integer(), reference(), iodata()) -> iodata().
await_data(Pid, Ref, Timeout, Mref, Acc) ->
	receive
		{gun_data, Pid, Ref, nofin, Data} ->
			await_data(Pid, Ref, Timeout, Mref, <<Acc/binary, Data/binary>>);
		{gun_data, Pid, Ref, fin, Data} ->
			<<Acc/binary, Data/binary>>;
		{gun_error, Pid, Ref, Reason} ->
			demonitor(Mref, [flush]),
			exit(Reason);
		{gun_error, Pid, Reason} ->
			demonitor(Mref, [flush]),
			exit(Reason);
		{'DOWN', Mref, process, Pid, Reason} ->
			exit(Reason)
	after Timeout ->
		demonitor(Mref, [flush]),
		exit(timeout)
	end.
