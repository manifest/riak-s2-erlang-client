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

-module(riaks2c_object).
-include_lib("riaks2c_xsd.hrl").

%% API
-export([
	fold/5,
	fold/6,
	list/3,
	list/4,
	expect_list/2,
	expect_list/3,
	head/4,
	head/5,
	expect_head/2,
	expect_head/3,
	expect_head/4,
	expect_body/2,
	expect_body/3,
	expect_body/4,
	get/4,
	get/5,
	await_get/2,
	await_get/3,
	expect_get/2,
	expect_get/3,
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

%% Types
-type fold_handler() :: fun(('ListEntry'(), any()) -> any()).

-export_type([fold_handler/0]).

%% =============================================================================
%% API
%% =============================================================================

-spec fold(pid(), iodata(), riaks2c:options(), any(), fold_handler()) -> reference().
fold(Pid, Bucket, Opts, Acc, Handle) ->
	fold(Pid, Bucket, #{}, Opts, Acc, Handle).

-spec fold(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options(), any(), fold_handler()) -> any().
fold(Pid, Bucket, ReqOpts, Opts, Acc0, Handle) ->
	Qs0 = maps:get(qs, ReqOpts, []),
	{MaxKeys, Qs1} =
		case lists:keyfind(<<"max-keys">>, 1, maps:get(qs, ReqOpts, [])) of
			false    -> {1000, [{<<"max-keys">>, <<"1000">>} | Qs0]};
			{_, Val} -> {binary_to_integer(Val), Qs0}
		end,

	#'ListBucketResult'{'Contents' = L} =
		riaks2c_object:expect_list(Pid, riaks2c_object:list(Pid, Bucket, ReqOpts#{qs => Qs1}, Opts)),
	case L of
		undefined ->
			%% There is no elements in the list
			%% so that our work is done.
			Acc0;
		_ ->
			case fold_lastsize(L) of
				{Last, MaxKeys} ->
					#'ListEntry'{'Key' = Marker} = Last,
					Acc1 = lists:foldl(Handle, Acc0, L),
					Qs2 = put_qsparam(Qs1, <<"marker">>, Marker),
					fold(Pid, Bucket, ReqOpts#{qs => Qs2}, Opts, Acc1, Handle);
				_X ->
					%% There is less elements then requested in the list
					%% so that our work is done.
					lists:foldl(Handle, Acc0, L)
			end
	end.

-spec list(pid(), iodata(), riaks2c:options()) -> reference().
list(Pid, Bucket, Opts) ->
	list(Pid, Bucket, #{}, Opts).

-spec list(pid(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
list(Pid, Bucket, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	NoSignQs = maps:get(qs, ReqOpts, []),
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, <<$/>>, [], NoSignQs, Bucket, Headers).

-spec expect_list(pid(), reference()) -> 'ListBucketResult'().
expect_list(Pid, Ref) ->
	expect_list(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec expect_list(pid(), reference(), non_neg_integer()) -> 'ListBucketResult'().
expect_list(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).

-spec head(pid(), iodata(), iodata(), riaks2c:options()) -> reference().
head(Pid, Bucket, Key, Opts) ->
	head(Pid, Bucket, Key, #{}, Opts).

-spec head(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
head(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:head(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers).

-spec expect_head(pid(), reference()) -> {riaks2c_http:status(), riaks2c_http:headers()}.
expect_head(Pid, Ref) ->
	expect_head(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec expect_head(pid(), reference(), non_neg_integer()) -> {riaks2c_http:status(), riaks2c_http:headers()}.
expect_head(Pid, Ref, Timeout) ->
	Mref = monitor(process, Pid),
	expect_head(Pid, Ref, Timeout, Mref).

-spec expect_head(pid(), reference(), non_neg_integer(), reference()) -> {riaks2c_http:status(), riaks2c_http:headers()}.
expect_head(Pid, Ref, Timeout, Mref) ->
	riaks2c_http:fold_head(
		Pid, Ref, Timeout, Mref,
		undefined,
		fun
			(inform, _IsFin, Status, Headers, _Acc) ->
				%% Ignore informational status codes. The tuple just makes dializer happy. 
				{Status, Headers};
			(response, nofin, Status, Headers, _Acc) ->
				%% Assuming we're using this function with riaks2c_object:get/{4,5},
				%% and riaks2c_object:expect_body/{2,3,4} will be called
				%% to flush stream messages and demonitor the stream process.
				{Status, Headers};
			(response, fin, Status, Headers, _Acc) ->
				demonitor(Mref, [flush]),
				gun:flush(Ref),
				{Status, Headers}
		end).

-spec expect_body(pid(), reference()) -> iodata().
expect_body(Pid, Ref) ->
	expect_body(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec expect_body(pid(), reference(), non_neg_integer()) -> iodata().
expect_body(Pid, Ref, Timeout) ->
	Mref = monitor(process, Pid),
	expect_body(Pid, Ref, Timeout, Mref).

-spec expect_body(pid(), reference(), non_neg_integer(), reference()) -> iodata().
expect_body(Pid, Ref, Timeout, Mref) ->
	riaks2c_http:await_body(Pid, Ref, Timeout, Mref).

-spec get(pid(), iodata(), iodata(), riaks2c:options()) -> reference().
get(Pid, Bucket, Key, Opts) ->
	get(Pid, Bucket, Key, #{}, Opts).

-spec get(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
get(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:get(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers).

-spec await_get(pid(), reference()) -> {ok, iodata()} | {error, any()}.
await_get(Pid, Ref) ->
	await_get(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_get(pid(), reference(), non_neg_integer()) -> {ok, iodata()} | {error, any()}.
await_get(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Bin) -> {ok, Bin};
		(206, _Hs, Bin) -> {ok, Bin};
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		(412, _Hs, Xml) -> riaks2c_http:return_response_error_412(Xml);
		(416, _Hs, Xml) -> riaks2c_http:return_response_error_416(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).

-spec expect_get(pid(), reference()) -> iodata().
expect_get(Pid, Ref) ->
	expect_get(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec expect_get(pid(), reference(), non_neg_integer()) -> iodata().
expect_get(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, Bin) -> Bin;
		(206, _Hs, Bin) -> Bin;
		(404, _Hs, Xml) -> riaks2c_http:throw_response_error_404(Xml);
		(412, _Hs, Xml) -> riaks2c_http:throw_response_error_412(Xml);
		(416, _Hs, Xml) -> riaks2c_http:throw_response_error_416(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).

-spec put(pid(), iodata(), iodata(), iodata(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, Val, Opts) ->
	put(Pid, Bucket, Key, Val, #{}, Opts).

-spec put(pid(), iodata(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
put(Pid, Bucket, Key, Val, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:put(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Val, Headers).

-spec await_put(pid(), reference()) -> ok | {error, any()}.
await_put(Pid, Ref) ->
	await_put(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_put(pid(), reference(), non_neg_integer()) -> ok | {error, any()}.
await_put(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _No) -> ok;
		(412, _Hs, Xml) -> riaks2c_http:return_response_error_412(Xml);
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
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

-spec await_copy(pid(), reference()) -> ok | {error, any()}.
await_copy(Pid, Ref) ->
	await_copy(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_copy(pid(), reference(), non_neg_integer()) -> ok | {error, any()}.
await_copy(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(200, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).

-spec remove(pid(), iodata(), iodata(), riaks2c_http:request_options()) -> reference().
remove(Pid, Bucket, Key, Opts) ->
	remove(Pid, Bucket, Key, #{}, Opts).

-spec remove(pid(), iodata(), iodata(), riaks2c_http:request_options(), riaks2c:options()) -> reference().
remove(Pid, Bucket, Key, ReqOpts, Opts) ->
	#{id := Id, secret := Secret, host := Host} = Opts,
	Headers = maps:get(headers, ReqOpts, []),
	riaks2c_http:delete(Pid, Id, Secret, Host, [<<$/>>, Key], Bucket, Headers).

-spec await_remove(pid(), reference()) -> ok | {error, any()}.
await_remove(Pid, Ref) ->
	await_remove(Pid, Ref, riaks2c_http:default_request_timeout()).

-spec await_remove(pid(), reference(), non_neg_integer()) -> ok | {error, any()}.
await_remove(Pid, Ref, Timeout) ->
	riaks2c_http:await(Pid, Ref, Timeout, fun
		(204, _Hs, _No) -> ok;
		(404, _Hs, Xml) -> riaks2c_http:return_response_error_404(Xml);
		( St, _Hs, Xml) -> riaks2c_http:throw_response_error(St, Xml)
	end).

%% =============================================================================
%% Internal functions
%% =============================================================================

-spec fold_lastsize(list()) -> {any(), non_neg_integer()}.
fold_lastsize(L) ->
	fold_lastsize(L, undefined, 0).

-spec fold_lastsize(list(), any(), non_neg_integer()) -> {any(), non_neg_integer()}.
fold_lastsize([Val|T], _Alast, Asize) -> fold_lastsize(T, Val, Asize +1);
fold_lastsize([], Alast, Asize)       -> {Alast, Asize}.

-spec put_qsparam(list(), binary(), binary()) -> list().
put_qsparam(Qs, Pkey, Pval) ->
	put_qsparam(Qs, Pkey, Pval, error, []).

-spec put_qsparam(list(), binary(), binary(), ok | error, list()) -> list().
put_qsparam([{Pkey, _Val}|T], Pkey, Pval, _Result, Acc) -> put_qsparam(T, Pkey, Pval, ok, [{Pkey, Pval}|Acc]);
put_qsparam([P|T], Pkey, Pval, Result, Acc)             -> put_qsparam(T, Pkey, Pval, Result, [P|Acc]);
put_qsparam([], Pkey, Pval, error, Acc)                 -> [{Pkey, Pval}|Acc];
put_qsparam([], _Pkey, _Pval, ok, Acc)                  -> Acc.
