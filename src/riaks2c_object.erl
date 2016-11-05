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
	find/4,
	find/5,
	get/4,
	get/5,
	put/5,
	put/6,
	copy/6,
	copy/7,
	remove/4,
	remove/5
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), iodata(), riaks2c:options()) -> 'ListBucketResult'().
list(Pid, Bucket, Opts) ->
	list(Pid, Bucket, #{}, Opts).

-spec list(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> 'ListBucketResult'().
list(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, <<$/>>, <<>>, list_qs(ReqOpts), Bucket, Headers, ReqOpts, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec find(pid(), iodata(), iodata(), riaks2c:options()) -> {ok, iodata()} | {error, any()}.
find(Pid, Bucket, Key, Opts) ->
	find(Pid, Bucket, Key, #{}, Opts).

-spec find(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> {ok, iodata()} | {error, any()}.
find(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	MethodFn = case maps:get(return_body, Opts, true) of true -> get; _ -> head end,
	riaks2c_http:MethodFn(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers, ReqOpts, fun
		(200, _Hs, Bin) -> Bin;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec get(pid(), iodata(), iodata(), riaks2c:options()) -> iodata().
get(Pid, Bucket, Key, Opts) ->
	get(Pid, Bucket, Key, #{}, Opts).

-spec get(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> iodata().
get(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	MethodFn = case maps:get(return_body, Opts, true) of true -> get; _ -> head end,
	riaks2c_http:MethodFn(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers, ReqOpts, fun
		(200, _Hs, Bin) -> Bin;
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml, Bucket, Key);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec put(pid(), iodata(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Key, Val, Opts) ->
	put(Pid, Bucket, Key, Val, #{}, Opts).

-spec put(pid(), iodata(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> ok | {error, any()}.
put(Pid, Bucket, Key, Val, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	ContentType = maps:get(content_type, ReqOpts, <<"application/octet-stream">>),
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Val, ContentType, Headers, ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec copy(pid(), iodata(), iodata(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
copy(Pid, Bucket, Key, SourceBucket, SourceKey, Opts) ->
	copy(Pid, Bucket, Key, SourceBucket, SourceKey, #{}, Opts).

-spec copy(pid(), iodata(), iodata(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> ok | {error, any()}.
copy(Pid, Bucket, Key, SourceBucket, SourceKey, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers =
		[	{<<"x-amz-copy-source">>, [<<$/>>, SourceBucket, <<$/>>, SourceKey]}
			| maps:get(headers, ReqOpts, []) ],
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers, ReqOpts, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket);
		(_St, _Hs, Xml) -> riaks2c_http:throw_response_error(Xml)
	end).

-spec remove(pid(), iodata(), iodata(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, Key, Opts) ->
	remove(Pid, Bucket, Key, #{}, Opts).

-spec remove(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> ok | {error, any()}.
remove(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:delete(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers, ReqOpts, fun
		(204, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml, Bucket, Key);
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
