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
	list/2,
	list/3,
	await_list/2,
	await_list/3,
	put/3,
	put/4,
	await_put/2,
	await_put/3,
	remove/3,
	remove/4,
	await_remove/2,
	await_remove/3
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), riaks2c:options()) -> reference().
list(Pid, Opts) ->
  list(Pid, #{}, Opts).

-spec list(pid(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
list(Pid, ReqOpts, Opts) ->
  #{id := Id, secret := Secret} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
  riaks2c_http:get(Pid, Id, Secret, <<$/>>, Headers).

-spec await_list(pid(), reference()) -> 'ListAllMyBucketsResult'().
await_list(Pid, Ref) ->
	await_list(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_list(pid(), reference(), timeout()) -> 'ListAllMyBucketsResult'().
await_list(Pid, Ref, Timeout) ->
  riaks2c_http:await(Pid, Ref, Timeout, fun
    (200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
    (_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
  end).

-spec put(pid(), iodata(), riaks2c:options()) -> reference().
put(Pid, Bucket, Opts) ->
	put(Pid, Bucket, #{}, Opts).

-spec put(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
put(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:put(Pid, Id, Secret, Host, <<$/>>, Bucket, Headers).

-spec await_put(pid(), reference()) -> ok.
await_put(Pid, Ref) ->
	await_put(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_put(pid(), reference(), timeout()) -> ok.
await_put(Pid, Ref, Timeout) ->
  riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _Xml) -> ok;
		(_St, _Hs, Xml)  -> riaks2c_http:throw_response_error(Xml)
  end).

-spec remove(pid(), iodata(), riaks2c:options()) -> reference().
remove(Pid, Bucket, Opts) ->
	remove(Pid, Bucket, #{}, Opts).

-spec remove(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
remove(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:delete(Pid, Id, Secret, Host, <<$/>>, Bucket, Headers).

-spec await_remove(pid(), reference()) -> ok | {error, any()}.
await_remove(Pid, Ref) ->
	await_remove(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_remove(pid(), reference(), timeout()) -> ok | {error, any()}.
await_remove(Pid, Ref, Timeout) ->
  riaks2c_http:await(Pid, Ref, Timeout, fun
		(204, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
  end).
