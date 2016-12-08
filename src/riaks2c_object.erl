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
	list/4,
	await_list/2,
	await_list/3,
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
	await_put/3,
	copy/6,
	copy/7,
	await_copy/2,
	await_copy/3,
	remove/4,
	remove/5,
	await_remove/2,
	await_remove/3
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
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, <<$/>>, <<>>, list_qs(ReqOpts), Bucket, Headers).

-spec await_list(pid(), reference()) -> 'ListBucketResult'().
await_list(Pid, Ref) ->
	await_list(Pid, Ref, #{}).

-spec await_list(pid(), reference(), riaks2c_http:request_options()) -> 'ListBucketResult'().
await_list(Pid, Ref, ReqOpts) ->
	Timeout = maps:get(request_timeout, ReqOpts, riaks2c_http:default_request_timeout()),
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec find(pid(), iodata(), iodata(), riaks2c:options()) -> reference().
find(Pid, Bucket, Key, Opts) ->
	find(Pid, Bucket, Key, #{}, Opts).

-spec find(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
find(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	MethodFn = case maps:get(return_body, Opts, true) of true -> get; _ -> head end,
	riaks2c_http:MethodFn(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers).

-spec await_find(pid(), reference()) -> {ok, iodata()} | {error, any()}.
await_find(Pid, Ref) ->
	await_find(Pid, Ref, #{}).

-spec await_find(pid(), reference(), riaks2c_http:request_options()) -> {ok, iodata()} | {error, any()}.
await_find(Pid, Ref, ReqOpts) ->
	Timeout = maps:get(request_timeout, ReqOpts, riaks2c_http:default_request_timeout()),
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Bin) -> Bin;
		(206, _Hs, Bin) -> Bin;
		(416, _Hs, _No) -> {error, bad_range};
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
	MethodFn = case maps:get(return_body, Opts, true) of true -> get; _ -> head end,
	riaks2c_http:MethodFn(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers).

-spec await_get(pid(), reference()) -> iodata().
await_get(Pid, Ref) ->
	await_get(Pid, Ref, #{}).

-spec await_get(pid(), reference(), riaks2c_http:request_options()) -> iodata().
await_get(Pid, Ref, ReqOpts) ->
	Timeout = maps:get(request_timeout, ReqOpts, riaks2c_http:default_request_timeout()),
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Bin) -> Bin;
		(206, _Hs, Bin) -> Bin;
		(416, _Hs, _No) -> exit(bad_range);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), iodata(), iodata(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, Val, Opts) ->
	put(Pid, Bucket, Key, Val, #{}, Opts).

-spec put(pid(), iodata(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, Val, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	ContentType = maps:get(content_type, ReqOpts, <<"application/octet-stream">>),
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Val, ContentType, Headers).

-spec await_put(pid(), reference(), riaks2c_http:request_options()) -> ok | {error, any()}.
await_put(Pid, Ref) ->
	await_put(Pid, Ref, #{}).

-spec await_put(pid(), reference()) -> ok | {error, any()}.
await_put(Pid, Ref, ReqOpts) ->
	Timeout = maps:get(request_timeout, ReqOpts, riaks2c_http:default_request_timeout()),
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec copy(pid(), iodata(), iodata(), iodata(), iodata(), riaks2c:options()) -> reference().
copy(Pid, Bucket, Key, SourceBucket, SourceKey, Opts) ->
	copy(Pid, Bucket, Key, SourceBucket, SourceKey, #{}, Opts).

-spec copy(pid(), iodata(), iodata(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
copy(Pid, Bucket, Key, SourceBucket, SourceKey, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers =
		[	{<<"x-amz-copy-source">>, [<<$/>>, SourceBucket, <<$/>>, SourceKey]}
			| maps:get(headers, ReqOpts, []) ],
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers).

-spec await_copy(pid(), reference(), riaks2c_http:request_options()) -> ok | {error, any()}.
await_copy(Pid, Ref) ->
	await_copy(Pid, Ref, #{}).

-spec await_copy(pid(), reference()) -> ok | {error, any()}.
await_copy(Pid, Ref, ReqOpts) ->
	Timeout = maps:get(request_timeout, ReqOpts, riaks2c_http:default_request_timeout()),
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec remove(pid(), iodata(), iodata(), riaks2c:options()) -> reference().
remove(Pid, Bucket, Key, Opts) ->
	remove(Pid, Bucket, Key, #{}, Opts).

-spec remove(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
remove(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:delete(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers).

-spec await_remove(pid(), reference(), riaks2c_http:request_options()) -> ok | {error, any()}.
await_remove(Pid, Ref) ->
	await_remove(Pid, Ref, #{}).

-spec await_remove(pid(), reference()) -> ok | {error, any()}.
await_remove(Pid, Ref, ReqOpts) ->
	Timeout = maps:get(request_timeout, ReqOpts, riaks2c_http:default_request_timeout()),
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(204, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec list_qs(riaks2c_http:request_options()) -> riak2c_http:qs().
list_qs(ReqOpts) ->
	list_qs(maps:to_list(ReqOpts), []).

-spec list_qs(list(), riaks2c_http:qs()) -> riaks2c_http:qs().
list_qs([{prefix, Val}|T], Acc)    -> list_qs(T, [{<<"prefix">>, Val} | Acc]);
list_qs([{delimiter, Val}|T], Acc) -> list_qs(T, [{<<"delimiter">>, Val} | Acc]);
list_qs([{marker, Val}|T], Acc)    -> list_qs(T, [{<<"marker">>, Val} | Acc]);
list_qs([{max_keys, Val}|T], Acc)  -> list_qs(T, [{<<"max-keys">>, integer_to_binary(Val)} | Acc]);
list_qs([_|T], Acc)                -> list_qs(T, Acc);
list_qs([], Acc)                   -> Acc.
