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

-module(bucket_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("riaks2c_xsd.hrl").

-compile(export_all).

%% =============================================================================
%% Common Test callbacks
%% =============================================================================

all() ->
	application:ensure_all_started(riaks2c),
	[{group, bucket}].

groups() ->
	[{bucket, [parallel], ct_helper:all(?MODULE)}].

init_per_suite(Config) ->
	riaks2c_cth:init_config() ++ Config.

%% =============================================================================
%% Tests
%% =============================================================================

bucket_putremove_roundtrip(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = riaks2c_cth:make_bucket(),
	ok = riaks2c_bucket:put(Pid, Bucket, Opts),
	ok = riaks2c_bucket:remove(Pid, Bucket, Opts),
	true.

bucket_list(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = riaks2c_cth:make_bucket(),
	ok = riaks2c_bucket:put(Pid, Bucket, Opts),
	Resp = riaks2c_bucket:list(Pid, Opts),
	ok = riaks2c_bucket:remove(Pid, Bucket, Opts),

	#'ListAllMyBucketsResult'{'Buckets' = #'ListAllMyBucketsList'{'Bucket' = L}} = Resp,
	lists:any(fun(#'ListAllMyBucketsEntry'{'Name' = Lbucket}) -> Lbucket =:= Bucket end, L).

bucket_acl_putget_roundtrip(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = riaks2c_cth:make_bucket(),

	#'ListAllMyBucketsResult'{'Owner' = Owner} = riaks2c_bucket:list(Pid, Opts),
	ACL =
		#'AccessControlPolicy'{
			'Owner' = Owner,
			'AccessControlList' =
				#'AccessControlList'{
					'Grant' =
						[	#'Grant'{
								'Grantee' = #'CanonicalUser'{'ID' = OwnerID} = Owner,
								'Permission' = <<"FULL_CONTROL">> } ]}},

	ok = riaks2c_bucket:put(Pid, Bucket, Opts),
	ok = riaks2c_bucket:update_acl(Pid, Bucket, ACL, Opts),
	Resp = riaks2c_bucket:get_acl(Pid, Bucket, Opts),
	ok = riaks2c_bucket:remove(Pid, Bucket, Opts),

	#'AccessControlPolicy'{
		'AccessControlList' =
			#'AccessControlList'{
				'Grant' =
					[	#'Grant'{
							'Grantee' = #'CanonicalUser'{'ID' = OwnerID},
							'Permission' = <<"FULL_CONTROL">> } ]}} = Resp,
	true.
