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

-module(object_multipart_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("riaks2c_xsd.hrl").

-compile(export_all).

%% =============================================================================
%% Common Test callbacks
%% =============================================================================

all() ->
	application:ensure_all_started(riaks2c),
	[{group, object_multipart}].

groups() ->
	[{object_multipart, [parallel], ct_helper:all(?MODULE)}].

init_per_suite(Config) ->
	riaks2c_cth:init_config() ++ Config.

init_per_testcase(_Test, Config) ->
	Opts = ?config(s2_user, Config),
	Bucket = riaks2c_cth:make_bucket(),
	Pid = riaks2c_cth:gun_open(Config),
	ok = riaks2c_bucket:await_put(Pid, riaks2c_bucket:put(Pid, Bucket, Opts)),
	[{bucket, Bucket} | Config].

end_per_testcase(_Test, Config) ->
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Pid = riaks2c_cth:gun_open(Config),
	ok = riaks2c_bucket:await_remove(Pid, riaks2c_bucket:remove(Pid, Bucket, Opts)).

end_per_suite(Config) ->
	Config.

%% =============================================================================
%% Tests
%% =============================================================================

complete_roundtrip(Config) ->
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),
	Payload = <<0:(5 *1024 *1024 *8)>>, %% 5Mb
	Pid = riaks2c_cth:gun_open(Config),

	%% Initializing multipart upload
	#'InitiateMultipartUploadResult'{'UploadId' = UploadId} =
		riaks2c_object_multipart:expect_init(Pid, riaks2c_object_multipart:init(Pid, Bucket, Key, Opts)),

	%% Uploading parts
	Etag1 =
		riaks2c_object_multipart:expect_put(
			Pid, riaks2c_object_multipart:put(Pid, Bucket, Key, Payload, UploadId, N1 = 1, Opts)),
	Etag3 =
		riaks2c_object_multipart:expect_put(
			Pid, riaks2c_object_multipart:put(Pid, Bucket, Key, Payload, UploadId, N3 = 3, Opts)),
	Etag2 =
		riaks2c_object_multipart:expect_put(
			Pid, riaks2c_object_multipart:put(Pid, Bucket, Key, Payload, UploadId, N2 = 2, Opts)),
	Parts = [{N1, Etag1}, {N2, Etag2}, {N3, Etag3}],

	%% Completing
	riaks2c_object_multipart:expect_complete(
		Pid,
		riaks2c_object_multipart:complete(Pid, Bucket, Key, UploadId, Parts, Opts)),
	
	%% Cleaning up
	riaks2c_object:await_remove(Pid, riaks2c_object:remove(Pid, Bucket, Key, Opts)).

cancel(Config) ->
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),
	Pid = riaks2c_cth:gun_open(Config),

	%% Initializing multipart upload
	#'InitiateMultipartUploadResult'{'UploadId' = UploadId} =
		riaks2c_object_multipart:expect_init(Pid, riaks2c_object_multipart:init(Pid, Bucket, Key, Opts)),

	%% Canceling
	riaks2c_object_multipart:expect_cancel(
		Pid, riaks2c_object_multipart:cancel(Pid, Bucket, Key, UploadId, Opts)),
	
	%% Checking
	#'ListMultipartUploadsResult'{'Upload' = undefined} =
		riaks2c_object_multipart:expect_list(Pid, riaks2c_object_multipart:list(Pid, Bucket, Opts)).

list(Config) ->
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),
	Pid = riaks2c_cth:gun_open(Config),

	%% Initializing multipart upload
	#'InitiateMultipartUploadResult'{'UploadId' = UploadId} =
		riaks2c_object_multipart:expect_init(Pid, riaks2c_object_multipart:init(Pid, Bucket, Key, Opts)),

	%% Checking
	#'ListMultipartUploadsResult'{'Upload' = L} =
		riaks2c_object_multipart:expect_list(Pid, riaks2c_object_multipart:list(Pid, Bucket, Opts)),
	1 = length([ok || #'MultipartUpload'{'UploadId' = Id} <- L, Id =:= UploadId]),
	
	%% Cleaning up
	riaks2c_object_multipart:expect_cancel(
		Pid, riaks2c_object_multipart:cancel(Pid, Bucket, Key, UploadId, Opts)).
