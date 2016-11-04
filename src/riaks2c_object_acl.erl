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
	find/5,
	find/6,
	get/5,
	get/6,
	put/6,
	put/7
]).

%% =============================================================================
%% API
%% =============================================================================

-spec find(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find(Pid, Bucket, Key, ReqOpts, Opts) ->
	find(Pid, Bucket, Key, [], ReqOpts, Opts).

-spec find(pid(), iodata(), iodata(), riak2c_http:headers(), riaks2c_http:request_options(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find(Pid, Bucket, Key, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Headers, ReqOpts, fun
		(200, _Hs, Xml) -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> 'AccessControlPolicy'().
get(Pid, Bucket, Key, ReqOpts, Opts) ->
	get(Pid, Bucket, Key, [], ReqOpts, Opts).

-spec get(pid(), iodata(), iodata(), riak2c_http:headers(), riaks2c_http:request_options(), riaks2c:options()) -> 'AccessControlPolicy'().
get(Pid, Bucket, Key, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Headers, ReqOpts, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), iodata(), 'AccessControlPolicy'(), riaks2c_http:request_options(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Key, ACL, ReqOpts, Opts) ->
	put(Pid, Bucket, Key, ACL, [], ReqOpts, Opts).

-spec put(pid(), iodata(), iodata(), 'AccessControlPolicy'(), riak2c_http:headers(), riaks2c_http:request_options(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Key, ACL, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	ContentType = <<"application/xml">>,
	Val = riaks2c_xsd:write(ACL),
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?acl">>], Bucket, Val, ContentType, Headers, ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

