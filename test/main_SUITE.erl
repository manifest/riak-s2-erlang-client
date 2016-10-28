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

-module(main_SUITE).
-include_lib("riaks2c_xsd.hrl").

-compile(export_all).

%% Helpers
-import(ct_helper, [
	config/2
]).
-import(riaks2c_test, [
	init_config/0,
	gun_open/1,
	gun_down/1
]).

%% Definitions
-define(BUCKET, <<"test-bucket">>).
-define(KEY, <<"test_file">>).
-define(VAL, <<"test_payload">>).
-define(CONTENT_TYPE, <<"application/octet-stream">>).

%% =============================================================================
%% Common Test callbacks
%% =============================================================================

all() ->
	application:ensure_all_started(riaks2c),
	ct_helper:all(?MODULE).

init_per_suite(Config) ->
	init_config() ++ Config.

%% =============================================================================
%% Tests
%% =============================================================================

bucket_list(Config) ->
	Pid = gun_open(Config),
	Opts = config(user, Config),
	#'ListAllMyBucketsResult'{} = riaks2c_bucket:list(Pid, Opts).

bucket_put(Config) ->
	Pid = gun_open(Config),
	Opts = config(user, Config),
	ok = riaks2c_bucket:put(Pid, ?BUCKET, Opts).

object_list(Config) ->
	Pid = gun_open(Config),
	Opts = config(user, Config),
	#'ListBucketResult'{} = riaks2c_object:list(Pid, ?BUCKET, Opts).

object_putget_roundtrip(Config) ->
	Pid = gun_open(Config),
	Opts = config(user, Config),
	ok = riaks2c_object:put(Pid, ?BUCKET, ?KEY, ?VAL, ?CONTENT_TYPE, Opts),
	?VAL = riaks2c_object:get(Pid, ?BUCKET, ?KEY, Opts).

