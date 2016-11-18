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

-module(riaks2c_bucket_policy).

%% API
-export([
	find/3,
	find/4,
	get/3,
	get/4,
	put/4,
	put/5,
	remove/3,
	remove/4
]).

%% =============================================================================
%% API
%% =============================================================================

-spec find(pid(), iodata(), riaks2c:options()) -> {ok, map()} | {error, any()}.
find(Pid, Bucket, Opts) ->
	find(Pid, Bucket, #{}, Opts).

-spec find(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> {ok, map()} | {error, any()}.
find(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, Xml) -> {ok, riaks2c_xsd:scan(Xml)};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get(pid(), iodata(), riaks2c:options()) -> map().
get(Pid, Bucket, Opts) ->
	get(Pid, Bucket, #{}, Opts).

-spec get(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> map().
get(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, Json) -> jsx:decode(Json, [return_maps]);
		(404, _Hs, Xml)  -> riaks2c_http:throw_response_error_404(Xml);
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), map(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Policy, Opts) ->
	put(Pid, Bucket, Policy, #{}, Opts).

%% FIXME: 'PUT Bucket Policy' request should return '204 No Content' status code on success.
%% http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/s3/put-bucket-policy
%% http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketPUTpolicy.html
-spec put(pid(), iodata(), map(), riaks2c_http:request_options(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Policy, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	ContentType = <<"application/json">>,
	Val = jsx:encode(Policy),
	riaks2c_http:put(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, Val, ContentType, Headers, ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec remove(pid(), iodata(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, Opts) ->
	remove(Pid, Bucket, #{}, Opts).

%% FIXME: 'DELETE Bucket Policy' request should return '204 No Content' status code on success.
%% http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/s3/delete-bucket-policy
%% http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketDELETEpolicy.html
-spec remove(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:delete(Pid, Id, Secret, Host, <<"/?policy">>, Bucket, Headers, ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).
