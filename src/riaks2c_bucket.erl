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

-module(riaks2c_bucket).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	%% main
	list/2,
	put/3,
	put/4,
	remove/3,
	%% acl
	find_acl/3,
	find_acl/4,
	get_acl/3,
	get_acl/4,
	update_acl/4,
	update_acl/5
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), riaks2c:options()) -> 'ListAllMyBucketsResult'().
list(Pid, Opts) ->
	#{id := Id, secret := Secret} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, <<$/>>, [], Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), riaks2c:options()) -> ok.
put(Pid, Bucket, Opts) ->
	put(Pid, Bucket, [], Opts).

-spec put(pid(), iodata(), cow_http:headers(), riaks2c:options()) -> ok.
put(Pid, Bucket, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:put(Pid, Id, Secret, Host, <<$/>>, Bucket, Headers, Timeout, fun
		(200, _Hs, _Xml) -> ok;
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).

-spec remove(pid(), iodata(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:delete(Pid, Id, Secret, Host, <<$/>>, Bucket, [], Timeout, fun
		(204, _Hs, _No)  -> ok;
		(404, _Hs, _Xml) -> riaks2c_http:return_response_error_404(Bucket);
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).

-spec find_acl(pid(), iodata(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find_acl(Pid, Bucket, Opts) ->
	find_acl(Pid, Bucket, [], Opts).

-spec find_acl(pid(), iodata(), cow_http:headers(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find_acl(Pid, Bucket, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Headers, Timeout, fun
		(200, _Hs, Xml)  -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, _Xml) -> riaks2c_http:return_response_error_404(Bucket);
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get_acl(pid(), iodata(), riaks2c:options()) -> 'AccessControlPolicy'().
get_acl(Pid, Bucket, Opts) ->
	get_acl(Pid, Bucket, [], Opts).

-spec get_acl(pid(), iodata(), cow_http:headers(), riaks2c:options()) -> 'AccessControlPolicy'().
get_acl(Pid, Bucket, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Headers, Timeout, fun
		(200, _Hs, Xml)  -> riaks2c_xsd:scan(Xml);
		(404, _Hs, _Xml) -> riaks2c_http:throw_response_error_404(Bucket);
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).

-spec update_acl(pid(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
update_acl(Pid, Bucket, ACL, Opts) ->
	update_acl(Pid, Bucket, ACL, [], Opts).

%% NOTE: change type of the ACL argument to 'AccessControlPolicy'() in the function spec
-spec update_acl(pid(), iodata(), any(), cow_http:headers(), riaks2c:options()) -> ok | {error, any()}.
update_acl(Pid, Bucket, ACL, Headers, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	ContentType = <<"application/xml">>,
	Val = riaks2c_xsd:write(ACL),
	riaks2c_http:put(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Val, ContentType, Headers, Timeout, fun
		(200, _Hs, _Xml) -> ok;
		(404, _Hs, _Xml) -> riaks2c_http:return_response_error_404(Bucket);
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).
