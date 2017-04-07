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
%% - Creating an object for all testcases but 'object_list', 'object_list_qs', 'object_put'
init_per_testcase(Test, Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = riaks2c_cth:make_bucket(),
	ok = riaks2c_bucket:await_put(Pid, riaks2c_bucket:put(Pid, Bucket, #{}, Opts)),
	case Test of
		object_list_qs  -> [{bucket, Bucket} | Config];
		object_list     -> [{bucket, Bucket} | Config];
		object_put      -> [{bucket, Bucket} | Config];
		object_put_data -> [{bucket, Bucket} | Config];
		_ ->
			Key = riaks2c_cth:make_key(),
			{Val, CT} = riaks2c_cth:make_content(),
			Headers = [{<<"content-type">>, CT}],
			ok = riaks2c_object:await_put(Pid, riaks2c_object:put(Pid, Bucket, Key, Val, #{headers => Headers}, Opts)),
			[{bucket, Bucket}, {key, Key} | Config]
	end.

%% - Removing the recently created bucket for all testcases
%% - Removing the recently created object for all testcases but 'object_list', 'object_list_qs'
end_per_testcase(Test, Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	case Test of
		object_list_qs  -> ok;
		object_list     -> ok;
		object_put      -> ok;
		object_put_data -> ok;
		_ ->
			Key = ?config(key, Config),
			ok = riaks2c_object:await_remove(Pid, riaks2c_object:remove(Pid, Bucket, Key, Opts))
	end,
	ok = riaks2c_bucket:await_remove(Pid, riaks2c_bucket:remove(Pid, Bucket, #{}, Opts)).

end_per_suite(Config) ->
	Config.

%% =============================================================================
%% Tests
%% =============================================================================

object_put(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),

	Key = riaks2c_cth:make_key(),
	{Val, CT} = riaks2c_cth:make_content(),
	Headers = [{<<"content-type">>, CT}],
	ok = riaks2c_object:await_put(Pid, riaks2c_object:put(Pid, Bucket, Key, Val, #{headers => Headers}, Opts)),

	Ref = riaks2c_object:get(Pid, Bucket, Key, Opts),
	{200, Hs} = riaks2c_object:expect_head(Pid, Ref),
	{_, CT} = lists:keyfind(<<"content-type">>, 1, Hs),
	Val = riaks2c_object:expect_body(Pid, Ref),
	ok = riaks2c_object:await_remove(Pid, riaks2c_object:remove(Pid, Bucket, Key, Opts)).

object_put_data(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),

	Size = 3,
	CT = <<"application/octet-stream">>,
	Headers = [{<<"content-type">>, CT}, {<<"content-length">>, integer_to_binary(Size)}],
	StreamRef = riaks2c_object:put(Pid, Bucket, Key, <<>>, #{headers => Headers}, Opts),
	gun:data(Pid, StreamRef, nofin, <<"1">>),
	gun:data(Pid, StreamRef, nofin, <<"2">>),
	gun:data(Pid, StreamRef, fin, <<"3">>),
	ok = riaks2c_object:await_put(Pid, StreamRef),

	Ref = riaks2c_object:get(Pid, Bucket, Key, Opts),
	{200, Hs} = riaks2c_object:expect_head(Pid, Ref),
	{_, CT} = lists:keyfind(<<"content-type">>, 1, Hs),
	<<"123">> = riaks2c_object:expect_body(Pid, Ref),
	ok = riaks2c_object:await_remove(Pid, riaks2c_object:remove(Pid, Bucket, Key, Opts)).

object_list(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = riaks2c_cth:make_key(),
	ExpectKey = iolist_to_binary(Key),
	IsObjectExist =
		fun() ->
			#'ListBucketResult'{'Contents' = Objects} = riaks2c_object:expect_list(Pid, riaks2c_object:list(Pid, Bucket, Opts)),
			case Objects of
				undefined -> false;
				_         -> lists:any(fun(#'ListEntry'{'Key' = Kval}) -> Kval =:= ExpectKey end, Objects)
			end
		end,

	false = IsObjectExist(),
	ok = riaks2c_object:await_put(Pid, riaks2c_object:put(Pid, Bucket, Key, <<42>>, Opts)),
	true = IsObjectExist(),
	ok = riaks2c_object:await_remove(Pid, riaks2c_object:remove(Pid, Bucket, Key, Opts)),
	false = IsObjectExist().

object_head(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = ?config(key, Config),

	{200, _Hs} = riaks2c_object:expect_head(Pid, riaks2c_object:head(Pid, Bucket, Key, Opts)).

object_headbody(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = ?config(key, Config),

	Ref = riaks2c_object:get(Pid, Bucket, Key, Opts),
	{200, _Hs} = riaks2c_object:expect_head(Pid, Ref),
	_Body = riaks2c_object:expect_body(Pid, Ref).

object_range_request(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = ?config(key, Config),

	ReqOpts = #{headers => [{<<"range">>, <<"bytes=0-1">>}]},
	2 = byte_size(riaks2c_object:expect_get(Pid, riaks2c_object:get(Pid, Bucket, Key, ReqOpts, Opts))).

object_range_request_error(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = ?config(key, Config),

	ReqOpts = #{headers => [{<<"range">>, <<"invalid">>}]},
	{error, bad_range} = riaks2c_object:await_get(Pid, riaks2c_object:get(Pid, Bucket, Key, ReqOpts, Opts)).

object_list_qs(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Keys =
		[[<<"group_a_">>, riaks2c_cth:make_key()] || _ <- lists:seq(1, 5)]
		++ [[<<"group_b_">>, riaks2c_cth:make_key()] || _ <- lists:seq(1, 3)],
	Tests =
		[	{8, []},
			{2, [{<<"max-keys">>, <<"2">>}]},
			{5, [{<<"prefix">>, <<"group_a_">>}]},
			{3, [{<<"prefix">>, <<"gr">>}, {<<"delimiter">>, <<"_a_">>}]},
			{3, [{<<"marker">>, <<"group_b_">>}]} ],

	[ok = riaks2c_object:await_put(Pid, riaks2c_object:put(Pid, Bucket, Key, <<42>>, Opts)) || Key <- Keys],
	[begin
		#'ListBucketResult'{'Contents' = Total} = riaks2c_object:expect_list(Pid, riaks2c_object:list(Pid, Bucket, #{qs => Qs}, Opts)),
		N = length(Total)
	end || {N, Qs} <- Tests],
	[riaks2c_object:await_remove(Pid, riaks2c_object:remove(Pid, Bucket, Key, Opts)) || Key <- Keys].

object_copy(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	ExpectedBucket = iolist_to_binary(Bucket),
	SourceKey = ?config(key, Config),
	DestKey = riaks2c_cth:make_key(),
	ExpectedDestKey = iolist_to_binary(DestKey),

	{error, {bad_key, ExpectedBucket, ExpectedDestKey}} = riaks2c_object:await_get(Pid, riaks2c_object:get(Pid, Bucket, DestKey, Opts)),
	ok = riaks2c_object:await_copy(Pid, riaks2c_object:copy(Pid, Bucket, DestKey, Bucket, SourceKey, Opts)),
	_ = riaks2c_object:expect_get(Pid, riaks2c_object:get(Pid, Bucket, DestKey, Opts)),
	ok = riaks2c_object:await_remove(Pid, riaks2c_object:remove(Pid, Bucket, DestKey, Opts)).

object_acl_roundtrip(Config) ->
	Pid = riaks2c_cth:gun_open(Config),
	Opts = ?config(s2_user, Config),
	Bucket = ?config(bucket, Config),
	Key = ?config(key, Config),

	#'ListAllMyBucketsResult'{'Owner' = ExpectedOwner} = riaks2c_bucket:expect_list(Pid, riaks2c_bucket:list(Pid, #{}, Opts)),
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

	{ok, _} = riaks2c_object_acl:await_get(Pid, riaks2c_object_acl:get(Pid, Bucket, Key, Opts)),
	ok = riaks2c_object_acl:await_put(Pid, riaks2c_object_acl:put(Pid, Bucket, Key, ACL, Opts)),
	#'AccessControlPolicy'{
		'AccessControlList' =
			#'AccessControlList'{
				'Grant' = [	#'Grant'{'Permission' = ExpectedPermission} ]}} = riaks2c_object_acl:expect_get(Pid, riaks2c_object_acl:get(Pid, Bucket, Key, Opts)).
