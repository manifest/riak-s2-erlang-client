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

-module(riaks2c).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	signature_v2/5,
	signature_v2/7,
	access_token_v2/2,
	handle_response/4,
	handle_response_error/1,
	handle_response_error_404/1,
	handle_response_error_404/3,
	default_request_timeout/0
]).

%% Types
-type options()          :: #{id => iodata(), secret => iodata(), host => iodata()}.
-type response_handler() :: fun((100..999, cow_http:headers(), iodata()) -> iodata()).

-export_type([options/0, response_handler/0]).

%% =============================================================================
%% API
%% =============================================================================

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

-spec handle_response_error(iodata()) -> no_return().
handle_response_error(Xml) ->
	exit(riaks2c_xsd:scan(Xml)).

-spec handle_response_error_404(iodata()) -> no_return().
handle_response_error_404(Bucket) ->
	exit({bad_bucket, Bucket}).

-spec handle_response_error_404(iodata(), iodata(), iodata()) -> no_return().
handle_response_error_404(Xml, Bucket, Key) ->
	#'Error'{'Code' = Code} = riaks2c_xsd:scan(Xml),
	case Code of
		<<"NoSuchKey">>    -> error({bad_key, Bucket, Key});
		<<"NoSuchBucket">> -> error({bad_bucket, Bucket})
	end.

-spec default_request_timeout() -> non_neg_integer().
default_request_timeout() ->
	5000.

%% =============================================================================
%% Internal functions
%% =============================================================================

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

-spec await_data(pid(), reference(), non_neg_integer(), reference(), iodata()) -> iodata().
await_data(Pid, Ref, Timeout, Mref, Acc) ->
	receive
		{gun_data, Pid, Ref, nofin, Data} ->
			await_data(Pid, Ref, Timeout, Mref, <<Data/binary, Acc/binary>>);
		{gun_data, Pid, Ref, fin, Data} ->
			<<Data/binary, Acc/binary>>;
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
