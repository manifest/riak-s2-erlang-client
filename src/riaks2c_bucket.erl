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
	list/3,
	put/4,
	put/5,
	remove/4,
	%% acl
	find_acl/4,
	find_acl/5,
	get_acl/4,
	get_acl/5,
	put_acl/5,
	put_acl/6,
	%%
	find_policy/4,
	find_policy/5,
	get_policy/4,
	get_policy/5,
	put_policy/5,
	put_policy/6,
	remove_policy/4
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), riaks2c:request_options(), riaks2c:options()) -> 'ListAllMyBucketsResult'().
list(Pid, ReqOpts, Opts) ->
	#{id := Id, secret := Secret} = Opts,
	riaks2c_http:get(Pid, Id, Secret, <<$/>>, [], ReqOpts, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), riaks2c:request_options(), riaks2c:options()) -> ok.
put(Pid, Bucket, ReqOpts, Opts) ->
	put(Pid, Bucket, [], ReqOpts, Opts).

-spec put(pid(), iodata(), cow_http:headers(), riaks2c:request_options(), riaks2c:options()) -> ok.
put(Pid, Bucket, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:put(Pid, Id, Secret, Host, <<$/>>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, _Xml) -> ok;
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).

-spec remove(pid(), iodata(), riaks2c:request_options(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:delete(Pid, Id, Secret, Host, <<$/>>, Bucket, [], ReqOpts, fun
		(204, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec find_acl(pid(), iodata(), riaks2c:request_options(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find_acl(Pid, Bucket, ReqOpts, Opts) ->
	find_acl(Pid, Bucket, [], ReqOpts, Opts).

-spec find_acl(pid(), iodata(), cow_http:headers(), riaks2c:request_options(), riaks2c:options()) -> {ok, 'AccessControlPolicy'()} | {error, any()}.
find_acl(Pid, Bucket, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, Xml) -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get_acl(pid(), iodata(), riaks2c:request_options(), riaks2c:options()) -> 'AccessControlPolicy'().
get_acl(Pid, Bucket, ReqOpts, Opts) ->
	get_acl(Pid, Bucket, [], ReqOpts, Opts).

-spec get_acl(pid(), iodata(), cow_http:headers(), riaks2c:request_options(), riaks2c:options()) -> 'AccessControlPolicy'().
get_acl(Pid, Bucket, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put_acl(pid(), iodata(), 'AccessControlPolicy'(), riaks2c:request_options(), riaks2c:options()) -> ok | {error, any()}.
put_acl(Pid, Bucket, ACL, ReqOpts, Opts) ->
	put_acl(Pid, Bucket, ACL, [], ReqOpts, Opts).

-spec put_acl(pid(), iodata(), 'AccessControlPolicy'(), cow_http:headers(), riaks2c:request_options(), riaks2c:options()) -> ok | {error, any()}.
put_acl(Pid, Bucket, ACL, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	ContentType = <<"application/xml">>,
	Val = riaks2c_xsd:write(ACL),
	riaks2c_http:put(Pid, Id, Secret, Host, <<"/?acl">>, Bucket, Val, ContentType, Headers, ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec find_policy(pid(), iodata(), riaks2c:request_options(), riaks2c:options()) -> {ok, map()} | {error, any()}.
find_policy(Pid, Bucket, ReqOpts, Opts) ->
	find_policy(Pid, Bucket, [], ReqOpts, Opts).

-spec find_policy(pid(), iodata(), cow_http:headers(), riaks2c:request_options(), riaks2c:options()) -> {ok, map()} | {error, any()}.
find_policy(Pid, Bucket, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, Xml) -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get_policy(pid(), iodata(), riaks2c:request_options(), riaks2c:options()) -> map().
get_policy(Pid, Bucket, ReqOpts, Opts) ->
	get_policy(Pid, Bucket, [], ReqOpts, Opts).

-spec get_policy(pid(), iodata(), cow_http:headers(), riaks2c:request_options(), riaks2c:options()) -> map().
get_policy(Pid, Bucket, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, Json) -> jsx:decode(Json, [return_maps]);
		(404, _Hs, Xml)  -> riaks2c_http:throw_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put_policy(pid(), iodata(), map(), riaks2c:request_options(), riaks2c:options()) -> ok | {error, any()}.
put_policy(Pid, Bucket, Policy, ReqOpts, Opts) ->
	put_policy(Pid, Bucket, Policy, [], ReqOpts, Opts).

%% FIXME: 'PUT Bucket Policy' request should return '204 No Content' status code on success.
%% http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/s3/put-bucket-policy
%% http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTpolicy.html
-spec put_policy(pid(), iodata(), map(), cow_http:headers(), riaks2c:request_options(), riaks2c:options()) -> ok | {error, any()}.
put_policy(Pid, Bucket, Policy, Headers, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	ContentType = <<"application/json">>,
	Val = jsx:encode(Policy),
	riaks2c_http:put(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, Val, ContentType, Headers, ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

%% FIXME: 'DELETE Bucket Policy' request should return '204 No Content' status code on success.
%% http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/s3/delete-bucket-policy
%% http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEpolicy.html
-spec remove_policy(pid(), iodata(), riaks2c:request_options(), riaks2c:options()) -> ok | {error, any()}.
remove_policy(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	riaks2c_http:delete(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, [], ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).
