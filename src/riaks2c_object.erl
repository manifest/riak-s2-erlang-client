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

-module(riaks2c_object).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	list/3,
	find/4,
	find/5,
	get/4,
	get/5,
	create/6,
	create/7,
	remove/4
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), iodata(), riaks2c:options()) -> 'ListBucketResult'().
list(Pid, Bucket, Opts) ->
	#{id := Id,
		secret := Secret,
		host := RootHost} = Opts,
	Sign =
		riaks2c:signature_v2(
			Secret,
			Method = <<"GET">>,
			[<<$/>>, Bucket, Path = <<$/>>],
			Date = cow_date:rfc7231(erlang:universaltime()),
			[]),
	Headers =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, RootHost]},
			{<<"authorization">>, riaks2c:access_token_v2(Id, Sign)} ],
	riaks2c:handle_response(
		Pid,
		gun:request(Pid, Method, Path, Headers),
		maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
		fun
			(200, _Hs, Xml)  -> riaks2c_xsd:scan(Xml);
			(404, _Hs, _Xml) -> riaks2c:throw_response_error_404(Bucket);
			(_St, _Hs, Xml)  -> riaks2c:throw_response_error(Xml)
		end).

-spec find(pid(), iodata(), iodata(), riaks2c:options()) -> {ok, iodata()} | {error, any()}.
find(Pid, Bucket, Key, Opts) ->
	find(Pid, Bucket, Key, [], Opts).

-spec find(pid(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> {ok, iodata()} | {error, any()}.
find(Pid, Bucket, Key, Headers, Opts) ->
	find_(Pid, Bucket, Key, Headers, Opts, fun
		(200, _Hs, Bin) -> Bin;
		(404, _Hs, Xml) -> riaks2c:return_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c:throw_response_error(Xml)
	end).

-spec get(pid(), iodata(), iodata(), riaks2c:options()) -> iodata().
get(Pid, Bucket, Key, Opts) ->
	get(Pid, Bucket, Key, [], Opts).

-spec get(pid(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> iodata().
get(Pid, Bucket, Key, Headers, Opts) ->
	find_(Pid, Bucket, Key, Headers, Opts, fun
		(200, _Hs, Bin) -> Bin;
		(404, _Hs, Xml) -> riaks2c:throw_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c:throw_response_error(Xml)
	end).

-spec create(pid(), iodata(), iodata(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
create(Pid, Bucket, Key, Val, ContentType, Opts) ->
	create(Pid, Bucket, Key, Val, ContentType, [], Opts).

-spec create(pid(), iodata(), iodata(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> ok | {error, any()}.
create(Pid, Bucket, Key, Val, ContentType, Headers0, Opts) ->
	#{id := Id,
		secret := Secret,
		host := RootHost} = Opts,
	Sign =
		riaks2c:signature_v2(
			Secret,
			Method = <<"PUT">>,
			[<<$/>>, Bucket, Path = [<<$/>>, Key]],
			ContentMD5 = base64:encode(erlang:md5(Val)),
			ContentType,
			Date = cow_date:rfc7231(erlang:universaltime()),
			Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, RootHost]},
			{<<"content-md5">>, ContentMD5},
			{<<"content-type">>, ContentType},
			{<<"authorization">>, riaks2c:access_token_v2(Id, Sign)}
			| Headers0 ],
	riaks2c:handle_response(
		Pid,
		gun:request(Pid, Method, Path, Headers1, Val),
		maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
		fun
			(200, _Hs, _Xml) -> ok;
			(404, _Hs, _Xml) -> riaks2c:return_response_error_404(Bucket);
			(_St, _Hs, Xml)  -> riaks2c:throw_response_error(Xml)
		end).

-spec remove(pid(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, Key, Opts) ->
	#{id := Id,
		secret := Secret,
		host := RootHost} = Opts,
	Sign =
		riaks2c:signature_v2(
			Secret,
			Method = <<"DELETE">>,
			[<<$/>>, Bucket, Path = [<<$/>>, Key]],
			Date = cow_date:rfc7231(erlang:universaltime()),
			[]),
	Headers =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, RootHost]},
			{<<"authorization">>, riaks2c:access_token_v2(Id, Sign)} ],
	riaks2c:handle_response(
		Pid,
		gun:request(Pid, Method, Path, Headers),
		maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
		fun
			(204, _Hs, _No) -> ok;
			(404, _Hs, Xml) -> riaks2c:return_response_error_404(Xml, Bucket, Key);
			(_St, _Hs, Xml) -> riaks2c:throw_response_error(Xml)
		end).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec find_(pid(), iodata(), iodata(), cow_http:headers(), riaks2c:options(), riaks2c:handle_response()) -> iodata() | {ok, iodata()} | {error, any()}.
find_(Pid, Bucket, Key, Headers0, Opts, Handle) ->
	#{id := Id,
		secret := Secret,
		host := RootHost} = Opts,
	Sign =
		riaks2c:signature_v2(
			Secret,
			Method = <<"GET">>,
			[<<$/>>, Bucket, Path = [<<$/>>, Key]],
			Date = cow_date:rfc7231(erlang:universaltime()),
			Headers0),
	Headers1 =
		[	{<<"date">>, Date},
			{<<"host">>, [Bucket, <<$.>>, RootHost]},
			{<<"authorization">>, riaks2c:access_token_v2(Id, Sign)}
			| Headers0 ],
	riaks2c:handle_response(
		Pid,
		gun:request(Pid, Method, Path, Headers1),
		maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
		Handle).
