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

-module(riaks2c_object_acl).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	find/4,
	find/5,
	await_find/2,
	await_find/3,
	get/4,
	get/5,
	await_get/2,
	await_get/3,
	put/5,
	put/6,
	await_put/2,
	await_put/3
]).

%% =============================================================================
%% API
%% =============================================================================

-spec find(pid(), iodata(), iodata(), riaks2c:options()) -> reference().
find(Pid, Bucket, Key, Opts) ->
	find(Pid, Bucket, Key, #{}, Opts).

-spec find(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
find(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Headers).

-spec await_find(pid(), reference()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
await_find(Pid, Ref) ->
	await_find(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_find(pid(), reference(), timeout()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
await_find(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get(pid(), iodata(), iodata(), riaks2c:options()) -> reference().
get(Pid, Bucket, Key, Opts) ->
	get(Pid, Bucket, Key, #{}, Opts).

-spec get(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
get(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Headers).

-spec await_get(pid(), reference()) -> 'AccessControlPolicy'().
await_get(Pid, Ref) ->
	await_get(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_get(pid(), reference(), timeout()) -> 'AccessControlPolicy'().
await_get(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), iodata(), 'AccessControlPolicy'(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, ACL, Opts) ->
	put(Pid, Bucket, Key, ACL, #{}, Opts).

-spec put(pid(), iodata(), iodata(), 'AccessControlPolicy'(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, ACL, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	ContentType = <<"application/xml">>,
	Val = riaks2c_xsd:write(ACL),
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Val, ContentType, Headers).

-spec await_put(pid(), reference(), timeout()) -> ok | {error, any()}.
await_put(Pid, Ref) ->
	await_put(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_put(pid(), reference()) -> ok | {error, any()}.
await_put(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).
