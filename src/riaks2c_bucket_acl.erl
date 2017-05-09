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

-module(riaks2c_bucket_acl).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	get/3,
	get/4,
	await_get/2,
	await_get/3,
	expect_get/2,
	expect_get/3,
	put/4,
	put/5,
	await_put/2,
	await_put/3
]).

%% =============================================================================
%% API
%% =============================================================================

-spec get(pid(), iodata(), riaks2c:options()) -> reference().
get(Pid, Bucket, Opts) ->
	get(Pid, Bucket, #{}, Opts).

-spec get(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
get(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Headers).

-spec await_get(pid(), reference()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
await_get(Pid, Ref) ->
	await_get(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_get(pid(), reference(), non_neg_integer()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
await_get(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).

-spec expect_get(pid(), reference()) -> 'AccessControlPolicy'().
expect_get(Pid, Ref) ->
	expect_get(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec expect_get(pid(), reference(), non_neg_integer()) -> 'AccessControlPolicy'().
expect_get(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).

-spec put(pid(), iodata(), 'AccessControlPolicy'(), riaks2c:options()) -> reference().
put(Pid, Bucket, ACL, Opts) ->
	put(Pid, Bucket, ACL, #{}, Opts).

-spec put(pid(), iodata(), 'AccessControlPolicy'(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
put(Pid, Bucket, ACL, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Val = riaks2c_xsd:write(ACL),
	Headers = [{<<"content-type">>, <<"application/xml">>} | maps:get(headers, ReqOpts, [])],
	riaks2c_http:put(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Val, Headers).

-spec await_put(pid(), reference()) -> ok | {error, any()}.
await_put(Pid, Ref) ->
	await_put(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_put(pid(), reference(), non_neg_integer()) -> ok | {error, any()}.
await_put(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).
