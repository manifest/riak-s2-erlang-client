%% ----------------------------------------------------------------------------
%% The MIT License
%%
%% Copyright (c) 2016-2017 Andrei Nesterov <ae.nesterov@gmail.com>
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
	put/8,
	post/7,
	post/8,
	delete/7,
	delete/9,
	cancel/2,
	flush/1,
	await/4,
	await/5,
	fold_head/6,
	await_body/4,
	fold_body/6,
	amz_headers/1,
	signature_v2/5,
	signature_v2/7,
	access_token_v2/2,
	throw_response_error/2,
	throw_response_error_400/1,
	throw_response_error_404/1,
	throw_response_error_412/1,
	throw_response_error_416/1,
	return_response_error_404/1,
	return_response_error_412/1,
	return_response_error_416/1,
	default_request_timeout/0
]).

%% Definitions
-define(DEFAULT_REQUEST_TIMEOUT, 5000).

%% Types
-type qs()               :: [{binary(), binary() | true}].
-type headers()          :: [{binary(), iodata()}].
-type status()           :: 100..999.
-type fin()              :: fin | nofin.
-type head_handler()     :: fun((inform | response, fin(), status(), headers(), any()) -> any()).
-type body_handler()     :: fun((fin(), binary(), any()) -> any()).
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

-spec get(pid(), iodata(), iodata(), iodata(), iodata(), qs(), qs(), iodata(), headers()) -> reference().
get(Pid, Id, Secret, Host, Path, SignQs, NoSignQs, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"GET">>, Host, Path, SignQs, NoSignQs, Bucket, Headers).

-spec put(pid(), iodata(), iodata(), iodata(), any(), iodata(), headers()) -> reference().
put(Pid, Id, Secret, Host, Path, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"PUT">>, Host, Path, Bucket, Headers).

-spec put(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), any(), headers()) -> reference().
put(Pid, Id, Secret, Host, Path, Bucket, Val, Headers) ->
	upload(Pid, Id, Secret, <<"PUT">>, Host, Path, Bucket, Val, Headers).

-spec post(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> reference().
post(Pid, Id, Secret, Host, Path, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"POST">>, Host, Path, Bucket, Headers).

-spec post(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> reference().
post(Pid, Id, Secret, Host, Path, Bucket, Val, Headers) ->
	upload(Pid, Id, Secret, <<"POST">>, Host, Path, Bucket, Val, Headers).

-spec delete(pid(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> reference().
delete(Pid, Id, Secret, Host, Path, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"DELETE">>, Host, Path, Bucket, Headers).

-spec delete(pid(), iodata(), iodata(), iodata(), iodata(), qs(), qs(), iodata(), headers()) -> reference().
delete(Pid, Id, Secret, Host, Path, SignQs, NoSignQs, Bucket, Headers) ->
	request(Pid, Id, Secret, <<"DELETE">>, Host, Path, SignQs, NoSignQs, Bucket, Headers).

-spec cancel(pid(), reference()) -> ok.
cancel(Pid, Ref) ->
	gun:cancel(Pid, Ref).

-spec flush(pid() | reference()) -> ok.
flush(PidOrRef) ->
	gun:flush(PidOrRef).

-spec await(pid(), reference(), non_neg_integer(), response_handler()) -> any().
await(Pid, Ref, Timeout, Handle) ->
	Mref = monitor(process, Pid),
	await(Pid, Ref, Timeout, Mref, Handle).

-spec await(pid(), reference(), non_neg_integer(), reference(), response_handler()) -> any().
await(Pid, Ref, Timeout, Mref, Handle) ->
	fold_head(
		Pid, Ref, Timeout, Mref,
		undefined,
		fun
			(_Type, nofin, Status, Headers, _Acc) ->
				Data = await_body(Pid, Ref, Timeout, Mref),
				Handle(Status, Headers, Data);
			(_Type, fin, Status, Headers, _Acc) ->
				demonitor(Mref, [flush]),
				flush(Ref),
				Handle(Status, Headers, <<>>)
		end).

%% Be careful, you must always flush stream messages and demonitor the stream proccess
%% in the 'Handle :: head_handler()' function. The function must not fail.
%% See riaks2c_http:await/5 as an example.
-spec fold_head(pid(), reference(), non_neg_integer(), reference(), any(), head_handler()) -> any().
fold_head(Pid, Ref, Timeout, Mref, Acc, Handle) ->
	receive
		{gun_inform, Pid, Ref, Status, Headers} ->
			fold_head(Pid, Ref, Timeout, Mref, Handle(inform, nofin, Status, Headers, Acc), Handle);
		{gun_response, Pid, Ref, IsFin, Status, Headers} ->
			Handle(response, IsFin, Status, Headers, Acc);
		{gun_error, Pid, Ref, Reason} ->
			demonitor(Mref, [flush]),
			flush(Ref),
			exit(Reason);
		{gun_error, Pid, Reason} ->
			demonitor(Mref, [flush]),
			flush(Ref),
			exit(Reason);
		{'DOWN', Mref, process, Pid, Reason} ->
			flush(Ref),
			exit(Reason)
	after Timeout ->
		demonitor(Mref, [flush]),
		flush(Ref),
		exit(timeout)
	end.

-spec await_body(pid(), reference(), non_neg_integer(), reference()) -> iodata().
await_body(Pid, Ref, Timeout, Mref) ->
	fold_body(Pid, Ref, Timeout, Mref, <<>>, fun accumulate_body/3).

%% Be careful, 'Handle :: body_handler()' function must not fail.
-spec fold_body(pid(), reference(), non_neg_integer(), reference(), any(), body_handler()) -> any().
fold_body(Pid, Ref, Timeout, Mref, Acc, Handle) ->
	receive
		{gun_data, Pid, Ref, nofin =IsFin, Data} ->
			fold_body(Pid, Ref, Timeout, Mref, Handle(IsFin, Data, Acc), Handle);
		{gun_data, Pid, Ref, fin =IsFin, Data} ->
			demonitor(Mref, [flush]),
			flush(Ref),
			Handle(IsFin, Data, Acc);
		{gun_error, Pid, Ref, Reason} ->
			demonitor(Mref, [flush]),
			flush(Ref),
			exit(Reason);
		{gun_error, Pid, Reason} ->
			demonitor(Mref, [flush]),
			flush(Ref),
			exit(Reason);
		{'DOWN', Mref, process, Pid, Reason} ->
			flush(Ref),
			exit(Reason)
	after Timeout ->
		demonitor(Mref, [flush]),
		flush(Ref),
		exit(timeout)
	end.

-spec amz_headers(list()) -> list().
amz_headers(Input) ->
	amz_headers(Input, ordsets:new()).

-spec signature_v2(iodata(), iodata(), iodata(), iodata(), headers()) -> iodata().
signature_v2(Secret, Method, Resource, Date, Headers) ->
	signature_v2(Secret, Method, Resource, <<>>, <<>>, Date, Headers).

-spec signature_v2(iodata(), iodata(), iodata(), iodata(), iodata(), iodata(), headers()) -> iodata().
signature_v2(Secret, Method, Resource, ContentMD5, ContentType, Date, AmzHeaders) ->
	Input =
		[	Method, <<$\n>>,
			ContentMD5, <<$\n>>,
			ContentType, <<$\n>>,
			Date, <<$\n>>,
			AmzHeaders,
			Resource ],
	base64:encode(crypto:hmac(sha, Secret, Input)).

-spec access_token_v2(iodata(), iodata()) -> iodata().
access_token_v2(Id, Sign) ->
	[<<"AWS ">>, Id, <<$:>>, Sign].

-spec throw_response_error(status(), iodata()) -> no_return().
throw_response_error(Status, Xml) ->
	exit({unsupported_response_error, {Status, try riaks2c_xsd:scan(Xml) catch _:_ -> Xml end}}).

-spec throw_response_error_400(iodata()) -> no_return().
throw_response_error_400(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	{Bucket, Key} = parse_resource_key(R), 
	case Code of
		<<"EntityTooSmall">>   -> error({multipart_invalid_part_size, Bucket, Key});
		<<"InvalidPart">>      -> error({multipart_invalid_part, Bucket, Key});
		<<"InvalidPartOrder">> -> error({multipart_invalid_part_order, Bucket, Key});
		_                      -> error({unsupported_response_error_code, Code})
	end.

-spec throw_response_error_404(iodata()) -> no_return().
throw_response_error_404(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchKey">>          -> {Bucket, Key} = parse_resource_key(R), error({bad_key, Bucket, Key});
		<<"NoSuchBucket">>       -> error({bad_bucket, parse_resource_bucket(R)});
		<<"NoSuchBucketPolicy">> -> error({bad_bucket_policy, parse_resource_bucket(R)});
		<<"NoSuchUpload">>       -> error(bad_upload_key);
		_                        -> error({unsupported_response_error_code, Code})
	end.

-spec throw_response_error_412(iodata()) -> no_return().
throw_response_error_412(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"PreconditionFailed">> -> {Bucket, Key} = parse_resource_key(R), error({bad_precondition, Bucket, Key});
		_                        -> error({unsupported_response_error_code, Code})
	end.

-spec throw_response_error_416(iodata()) -> no_return().
throw_response_error_416(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"InvalidRange">> -> {Bucket, Key} = parse_resource_key(R), error({bad_range, Bucket, Key});
		_                  -> error({unsupported_response_error_code, Code})
	end.

-spec return_response_error_404(iodata()) -> {error, any()}.
return_response_error_404(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchKey">>          -> {Bucket, Key} = parse_resource_key(R), {error, {bad_key, Bucket, Key}};
		<<"NoSuchBucket">>       -> {error, {bad_bucket, parse_resource_bucket(R)}};
		<<"NoSuchBucketPolicy">> -> {error, {bad_bucket_policy, parse_resource_bucket(R)}};
		<<"NoSuchUpload">>       -> {error, bad_upload_key};
		_                        -> error({unsupported_response_error_code, Code})
	end.

-spec return_response_error_412(iodata()) -> {error, any()}.
return_response_error_412(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"PreconditionFailed">> -> {Bucket, Key} = parse_resource_key(R), {error, {bad_precondition, Bucket, Key}};
		_                        -> error({unsupported_response_error_code, Code})
	end.

-spec return_response_error_416(iodata()) -> {error, any()}.
return_response_error_416(Xml) ->
	#'Error'{'Code' = Code, 'Resource' = R} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"InvalidRange">> -> {Bucket, Key} = parse_resource_key(R), {error, {bad_range, Bucket, Key}};
		_                  -> error({unsupported_response_error_code, Code})
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
	Sign = signature_v2(Secret, Method, Path, Date, amz_headers(Headers0)),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	gun:request(Pid, Method, Path, Headers1).

-spec request(pid(), iodata(), iodata(), binary(), iodata(), iodata(), iodata(), headers()) -> reference().
request(Pid, Id, Secret, Method, Host, Path, Bucket, Headers0) ->
	Date = cow_date:rfc7231(erlang:universaltime()),
	Sign = signature_v2(Secret, Method, [<<$/>>, Bucket, Path], Date, amz_headers(Headers0)),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, Host]},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	gun:request(Pid, Method, Path, Headers1).

%% This `request` function is used for requests that contain query string parameters
%% and subresource. The problem is that subresource is also represented as
%% query string parameter. While only subresource and query string parameters of
%% cancel multipart request (surprise-surprise) are used for building request's signature,
%% we need a way to distinguish them from other parameters.
%% So that,
%%   `SignQs` - query string parameters (including subresource) that need to be signed;
%%   `NoSignQs` - query string parameters that are used only in the request's path.
%%
%% For all other cases we use other `request` functions, passing subresource
%% withing `Path` argument if needed. This way a subresource will be presented
%% in request's path and signature.
-spec request(pid(), iodata(), iodata(), binary(), iodata(), iodata(), qs(), qs(), iodata(), headers()) -> reference().
request(Pid, Id, Secret, Method, Host, Path, SignQs, NoSignQs, Bucket, Headers0) ->
	Date = cow_date:rfc7231(erlang:universaltime()),
	Sign = signature_v2(Secret, Method, resource([<<$/>>, Bucket, Path], SignQs), Date, amz_headers(Headers0)),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, Host]},
			{<<"authorization">>, access_token_v2(Id, Sign)}
			| Headers0 ],
	gun:request(Pid, Method, path(Path, SignQs, NoSignQs), Headers1).

%% We set 'Content-MD5' header for all requests but those that don't have payload.
-spec upload(pid(), iodata(), iodata(), binary(), iodata(), iodata(), iodata(), any(), headers()) -> reference().
upload(Pid, Id, Secret, Method, Host, Path, Bucket, <<>>, Headers0) ->
	Date = cow_date:rfc7231(erlang:universaltime()),
	Resource = [<<$/>>, Bucket, Path],
	BucketHost = [Bucket, <<$.>>, Host],
	Headers1 =
		case content_headers(Headers0) of
			{AmzHeaders, {ok, CT}, {ok, _CL}, _MaybeCS} -> [{<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, <<>>, CT, Date, AmzHeaders))} | Headers0];
			{AmzHeaders, {ok, CT},     error, _MaybeCS} -> [{<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, <<>>, CT, Date, AmzHeaders))} | Headers0];
			{AmzHeaders,    error,  _MaybeCL, _MaybeCS} -> [{<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, <<>>, <<>>, Date, AmzHeaders))} | Headers0]
		end,
	gun:request(Pid, Method, Path, Headers1);
upload(Pid, Id, Secret, Method, Host, Path, Bucket, Val, Headers0) ->
	Date = cow_date:rfc7231(erlang:universaltime()),
	Resource = [<<$/>>, Bucket, Path],
	BucketHost = [Bucket, <<$.>>, Host],
	Headers1 =
		case content_headers(Headers0) of
			{AmzHeaders, {ok, CT}, {ok, _CL}, {ok, CS}} ->                                                  [                                                     {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, CT, Date, AmzHeaders))} | Headers0];
			{AmzHeaders,    error, {ok, _CL}, {ok, CS}} ->                                                  [                                                     {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, <<>>, Date, AmzHeaders))} | Headers0];
			{AmzHeaders, {ok, CT},    error,  {ok, CS}} -> CL = content_length(Val),                        [{<<"content-length">>, CL},                          {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, CT, Date, AmzHeaders))} | Headers0];
			{AmzHeaders,    error,    error,  {ok, CS}} -> CL = content_length(Val),                        [{<<"content-length">>, CL},                          {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, <<>>, Date, AmzHeaders))} | Headers0];
			{AmzHeaders, {ok, CT}, {ok, _CL},    error} ->                           CS = content_md5(Val), [                            {<<"content-md5">>, CS}, {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, CT, Date, AmzHeaders))} | Headers0];
			{AmzHeaders,    error, {ok, _CL},    error} ->                           CS = content_md5(Val), [                            {<<"content-md5">>, CS}, {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, <<>>, Date, AmzHeaders))} | Headers0];
			{AmzHeaders, {ok, CT},    error,     error} -> CL = content_length(Val), CS = content_md5(Val), [{<<"content-length">>, CL}, {<<"content-md5">>, CS}, {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, CT, Date, AmzHeaders))} | Headers0];
			{AmzHeaders,    error,    error,     error} -> CL = content_length(Val), CS = content_md5(Val), [{<<"content-length">>, CL}, {<<"content-md5">>, CS}, {<<"date">>, Date}, {<<"host">>, BucketHost}, {<<"authorization">>, access_token_v2(Id, signature_v2(Secret, Method, Resource, CS, <<>>, Date, AmzHeaders))} | Headers0]
		end,
	gun:request(Pid, Method, Path, Headers1, Val).

-spec path(iodata(), qs(), qs()) -> iodata().
path(Path, [], [])           -> Path;
path(Path, SignQs, [])       -> [Path, <<$?>>, cow_qs:qs(SignQs)];
path(Path, [], NoSignQs)     -> [Path, <<$?>>, cow_qs:qs(NoSignQs)];
path(Path, SignQs, NoSignQs) -> [Path, <<$?>>, cow_qs:qs(SignQs), <<$&>>, cow_qs:qs(NoSignQs)].

-spec resource(iodata(), qs()) -> iodata().
resource(Path, [])     -> Path;
resource(Path, SignQs) -> [Path, <<$?>>, qs_nourlencode(SignQs)].

-spec qs_nourlencode(qs()) -> iodata().
qs_nourlencode(Qs) ->
	qs_nourlencode(Qs, []).

-spec qs_nourlencode(qs(), iodata()) -> iodata().
qs_nourlencode([{Key, true}|T], Acc) -> qs_nourlencode(T, [Key|Acc]);
qs_nourlencode([{Key, Val}|T], Acc)  -> qs_nourlencode(T, [[Key, <<$=>>, Val]|Acc]);
qs_nourlencode([Key|T], Acc)         -> qs_nourlencode(T, [Key|Acc]);
qs_nourlencode([], Acc)              -> lists:reverse(Acc).

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

-spec accumulate_body(fin(), binary(), binary()) -> binary().
accumulate_body(_IsFin, Data, Acc) ->
	<<Acc/binary, Data/binary>>.

-spec amz_headers(headers(), headers()) -> headers().
amz_headers([{<<"x-amz-", _/bits>> =Key, Val}|T], L) -> amz_headers(T, ordsets:add_element([Key, <<$:>>, Val, <<$\n>>], L));
amz_headers([_|T], L)                                -> amz_headers(T, L);
amz_headers([], L)                                   -> L.

-spec content_headers(headers()) -> {headers(), MaybeH, MaybeH, MaybeH} when MaybeH :: {ok, iodata()} | error.
content_headers(Headers) ->
	content_headers(Headers, ordsets:new(), error, error, error).

-spec content_headers(headers(), headers(), MaybeH, MaybeH, MaybeH) -> {headers(), MaybeH, MaybeH, MaybeH} when MaybeH :: {ok, iodata()} | error.
content_headers([{<<"content-type">>, Val}|T], Hamz, _CT, CL, CS)       -> content_headers(T, Hamz, {ok, Val}, CL, CS);
content_headers([{<<"content-length">>, Val}|T], Hamz, CT, _CL, CS)     -> content_headers(T, Hamz, CT, {ok, Val}, CS);
content_headers([{<<"content-md5">>, Val}|T], Hamz, CT, CL, _CS)        -> content_headers(T, Hamz, CT, CL, {ok, Val});
content_headers([{<<"x-amz-", _/bits>> =Key, Val}|T], Hamz, CT, CL, CS) -> content_headers(T, ordsets:add_element([Key, <<$:>>, Val, <<$\n>>], Hamz), CT, CL, CS);
content_headers([_|T], Hamz, CT, CL, CS)                                -> content_headers(T, Hamz, CT, CL, CS);
content_headers(_, Hamz, CT, CL, CS)                                    -> {Hamz, CT, CL, CS}.

-spec content_length(iodata()) -> binary().
content_length(Val) ->
	integer_to_binary(iolist_size(Val)).

-spec content_md5(iodata()) -> binary().
content_md5(Val) ->
	base64:encode(erlang:md5(Val)).
