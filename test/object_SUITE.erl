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

-module(object_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("riaks2c_xsd.hrl").

-compile(export_all).

%% =============================================================================
%% Common Test callbacks
%% =============================================================================

all() ->
	application:ensure_all_started(riaks2c),
	[{group, object}].

groups() ->
	[{object, [parallel], ct_helper:all(?MODULE)}].

init_per_suite(Config) ->
	riaks2c_cth:init_config() ++ Config.

init_per_testcase(_Name, Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = riaks2c_cth:make_bucket(),
	ok = riaks2c_bucket:put(Pid, Bucket, Opts),
	[{bucket, Bucket} | Config].

end_per_testcase(_Name, Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = ?config(bucket, Config),
	ok = riaks2c_bucket:remove(Pid, Bucket, Opts).

%% =============================================================================
%% Tests
%% =============================================================================

object_createremove_roundtrip(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),
	{Val, ContentType} = riaks2c_cth:make_content(),
	ok = riaks2c_object:create(Pid, Bucket, Key, Val, ContentType, Opts),
	ok = riaks2c_object:remove(Pid, Bucket, Key, Opts),
	true.

object_list(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),
	{Val, ContentType} = riaks2c_cth:make_content(),
	ok = riaks2c_object:create(Pid, Bucket, Key, Val, ContentType, Opts),
	Resp = riaks2c_object:list(Pid, Bucket, Opts),
	ok = riaks2c_object:remove(Pid, Bucket, Key, Opts),

	#'ListBucketResult'{'Contents' = L} = Resp,
	lists:any(fun(#'ListEntry'{'Key' = Lkey}) -> Lkey =:= Key end, L).
