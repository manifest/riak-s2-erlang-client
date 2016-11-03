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

%% - Creating a bucket for all testcases
%% - Creating an object for all testcases but 'object_list'
init_per_testcase(Test, Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = riaks2c_cth:make_bucket(),
	ok = riaks2c_bucket:put(Pid, Bucket, #{}, Opts),
	case Test of
		object_list ->
			[{bucket, Bucket} | Config];
		_ ->
			Key = riaks2c_cth:make_key(),
			{Val, ContentType} = riaks2c_cth:make_content(),
			ok = riaks2c_object:put(Pid, Bucket, Key, Val, ContentType, #{}, Opts),
			[{bucket, Bucket}, {key, Key} | Config]
	end.

%% - Removing the recently created bucket for all testcases
%% - Removing the recently created object for all testcases but 'object_list'
end_per_testcase(Test, Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = ?config(bucket, Config),
	case Test of
		object_list ->
			ok;
		_ ->
			Key = ?config(key, Config),
			ok = riaks2c_object:remove(Pid, Bucket, Key, #{}, Opts)
	end,
	ok = riaks2c_bucket:remove(Pid, Bucket, #{}, Opts).

%% =============================================================================
%% Tests
%% =============================================================================

object_list(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),
	ExpectKey = iolist_to_binary(Key),
	{Val, ContentType} = riaks2c_cth:make_content(),
	IsObjectExist =
		fun() ->
			#'ListBucketResult'{'Contents' = Objects} = riaks2c_object:list(Pid, Bucket, #{}, Opts),
			case Objects of
				undefined -> false;
				_         -> lists:any(fun(#'ListEntry'{'Key' = Kval}) -> Kval =:= ExpectKey end, Objects)
			end
		end,

	false = IsObjectExist(),
	ok = riaks2c_object:put(Pid, Bucket, Key, Val, ContentType, #{}, Opts),
	true = IsObjectExist(),
	ok = riaks2c_object:remove(Pid, Bucket, Key, #{}, Opts),
	false = IsObjectExist(),
	true.

object_copy(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = ?config(bucket, Config),
	SourceKey = ?config(key, Config),
	DestKey = riaks2c_cth:make_key(),

	{error, {bad_key, Bucket, DestKey}} = riaks2c_object:find(Pid, Bucket, DestKey, #{return_body => false}, Opts),
	ok = riaks2c_object:copy(Pid, Bucket, DestKey, Bucket, SourceKey, #{}, Opts),
	_ = riaks2c_object:get(Pid, Bucket, DestKey, #{return_body => false}, Opts),
	ok = riaks2c_object:remove(Pid, Bucket, DestKey, #{}, Opts),
	true.

object_acl_roundtrip(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(user, Config),
	Bucket = ?config(bucket, Config),
	Key = ?config(key, Config),

	#'ListAllMyBucketsResult'{'Owner' = ExpectedOwner} = riaks2c_bucket:list(Pid, #{}, Opts),
	ExpectedPermission = <<"READ_ACP">>,
	ACL =
		#'AccessControlPolicy'{
			'Owner' = ExpectedOwner,
			'AccessControlList' =
				#'AccessControlList'{
					'Grant' =
						[	#'Grant'{
								'Grantee' = ExpectedOwner,
								'Permission' = ExpectedPermission } ]}},

	{ok, _} = riaks2c_object:find_acl(Pid, Bucket, Key, #{}, Opts),
	ok = riaks2c_object:put_acl(Pid, Bucket, Key, ACL, #{}, Opts),
	#'AccessControlPolicy'{
		'AccessControlList' =
			#'AccessControlList'{
				'Grant' = [	#'Grant'{'Permission' = ExpectedPermission} ]}} = riaks2c_object:get_acl(Pid, Bucket, Key, #{}, Opts),

	true.
