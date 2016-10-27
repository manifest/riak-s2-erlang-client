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
	put/3,
	put/4
]).

%% =============================================================================
%% API
%% =============================================================================

-spec list(pid(), riaks2c:options()) -> 'ListAllMyBucketsResult'().
list(Pid, Opts) ->
	#{id := Id,
		secret := Secret} = Opts,
	Method = <<"GET">>,
	Path = Resource = <<$/>>,
	Date = cow_date:rfc7231(calendar:universal_time()),
	Sign = riaks2c:signature_v2(Secret, Method, Resource, Date, []),
	Token = riaks2c:access_token_v2(Id, Sign),
	Headers = [{<<"date">>, Date}, {<<"authorization">>, Token}],
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	Ref = gun:request(Pid, Method, Path, Headers),
	riaks2c:handle_response(Pid, Ref, Timeout, fun
		(200, _Hs, Xml) -> riaks2c_xsd:scan(Xml);
		(_St, _Hs, Xml) -> riaks2c:handle_response_error(Xml)
	end).

-spec put(pid(), iodata(), riaks2c:options()) -> ok.
put(Pid, Bucket, Opts) ->
	put(Pid, Bucket, [], Opts).

-spec put(pid(), iodata(), cow_http:headers(), riaks2c:options()) -> ok.
put(Pid, Bucket, Headers0, Opts) ->
	#{id := Id,
		secret := Secret,
		host := RootHost} = Opts,
	Method = <<"PUT">>,
	Path = <<$/>>,
	Resource = [<<$/>>, Bucket, <<$/>>],
	Date = cow_date:rfc7231(calendar:universal_time()),
	Sign = riaks2c:signature_v2(Secret, Method, Resource, Date, Headers0),
	Token = riaks2c:access_token_v2(Id, Sign),
	Host = [Bucket, <<$.>>, RootHost],
	Headers1 = [{<<"date">>, Date}, {<<"host">>, Host}, {<<"authorization">>, Token} | Headers0],
	Timeout = maps:get(request_timeout, Opts, riaks2c:default_request_timeout()),
	Ref = gun:request(Pid, Method, Path, Headers1),
	riaks2c:handle_response(Pid, Ref, Timeout, fun
		(200, _Hs, _Xml) -> ok;
		(_St, _Hs, Xml)  -> riaks2c:handle_response_error(Xml)
	end).
