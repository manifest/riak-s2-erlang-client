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

-module(riaks2c_object_multipart).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	list/3,
	list/4,
	expect_list/2,
	expect_list/3,
	init/4,
	init/5,
	expect_init/2,
	expect_init/3,
	put/7,
	put/8,
	await_put/2,
	await_put/3
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), iodata(), riaks2c:options()) -> reference().
list(Pid, Bucket, Opts) ->
	list(Pid, Bucket, #{}, Opts).

-spec list(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
list(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Qs = maps:get(qs, ReqOpts, []),
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, <<$/>>, <<"uploads">>, Qs, Bucket, Headers).

-spec expect_list(pid(), reference()) -> 'ListMultipartUploadsResult'().
expect_list(Pid, Ref) ->
	expect_list(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec expect_list(pid(), reference(), non_neg_integer()) -> 'ListMultipartUploadsResult'().
expect_list(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec init(pid(), iodata(), iodata(), riaks2c:options()) -> reference().
init(Pid, Bucket, Key, Opts) ->
	init(Pid, Bucket, Key, #{}, Opts).

-spec init(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
init(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:post(Pid, Id, Secret, Host, [<<$/>>, Key, <<"?uploads">>], Bucket, Headers).

-spec expect_init(pid(), reference()) -> iodata().
expect_init(Pid, Ref) ->
	expect_init(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec expect_init(pid(), reference(), non_neg_integer()) -> iodata().
expect_init(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), iodata(), iodata(), iodata(), non_neg_integer(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, Val, UploadId, PartNumber, Opts) ->
	riaks2c_http:put(Pid, Bucket, Key, Val, UploadId, PartNumber, #{}, Opts).

-spec put(pid(), iodata(), iodata(), iodata(), iodata(), non_neg_integer(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, Val, UploadId, PartNumber, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	Path = [<<$/>>, Key, <<"?partNumber=">>, integer_to_binary(PartNumber), <<"&uploadId=">>, UploadId],
	riaks2c_http:put(Pid, Id, Secret, Host, Path, Bucket, Val, Headers).

-spec await_put(pid(), reference()) -> ok | {error, any()}.
await_put(Pid, Ref) ->
	await_put(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_put(pid(), reference(), non_neg_integer()) -> ok | {error, any()}.
await_put(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).
