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
	%% main
	list/3,
	find/4,
	find/5,
	get/4,
	get/5,
	put/6,
	put/7,
	remove/4,
	%% acl
	find_acl/4,
	find_acl/5,
	get_acl/4,
	get_acl/5,
	put_acl/5,
	put_acl/6
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), iodata(), riaks2c:options()) -> 'ListBucketResult'().
list(Pid, Bucket, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, Host, <<$/>>, Bucket, [], Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec find(pid(), iodata(), iodata(), riaks2c:options()) -> {ok, iodata()} | {error, any()}.
find(Pid, Bucket, Key, Opts) ->
	find(Pid, Bucket, Key, [], Opts).

-spec find(pid(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> {ok, iodata()} | {error, any()}.
find(Pid, Bucket, Key, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers, Timeout, fun
		(200, _Hs, Bin) -> Bin;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get(pid(), iodata(), iodata(), riaks2c:options()) -> iodata().
get(Pid, Bucket, Key, Opts) ->
	get(Pid, Bucket, Key, [], Opts).

-spec get(pid(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> iodata().
get(Pid, Bucket, Key, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers, Timeout, fun
		(200, _Hs, Bin) -> Bin;
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), iodata(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Key, Val, ContentType, Opts) ->
	put(Pid, Bucket, Key, Val, ContentType, [], Opts).

-spec put(pid(), iodata(), iodata(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Key, Val, ContentType, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Val, ContentType, Headers, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec remove(pid(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, Key, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:delete(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, [], Timeout, fun
		(204, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec find_acl(pid(), iodata(), iodata(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find_acl(Pid, Bucket, Key, Opts) ->
	find_acl(Pid, Bucket, Key, [], Opts).

-spec find_acl(pid(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find_acl(Pid, Bucket, Key, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Headers, Timeout, fun
		(200, _Hs, Xml) -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get_acl(pid(), iodata(), iodata(), riaks2c:options()) -> 'AccessControlPolicy'().
get_acl(Pid, Bucket, Key, Opts) ->
	get_acl(Pid, Bucket, Key, [], Opts).

-spec get_acl(pid(), iodata(), iodata(), cow_http:headers(), riaks2c:options()) -> 'AccessControlPolicy'().
get_acl(Pid, Bucket, Key, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Headers, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put_acl(pid(), iodata(), iodata(), 'AccessControlPolicy'(), riaks2c:options()) -> ok | {error, any()}.
put_acl(Pid, Bucket, Key, ACL, Opts) ->
	put_acl(Pid, Bucket, Key, ACL, [], Opts).

-spec put_acl(pid(), iodata(), iodata(), 'AccessControlPolicy'(), cow_http:headers(), riaks2c:options()) -> ok | {error, any()}.
put_acl(Pid, Bucket, Key, ACL, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	ContentType = <<"application/xml">>,
	Val = riaks2c_xsd:write(ACL),
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Val, ContentType, Headers, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).
