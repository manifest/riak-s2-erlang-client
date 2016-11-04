# Riak S2 Client

[![Build Status][travis-img]][travis]

Erlang client for Riak S2.
You can view API documentation [here][riak-s2-docs].
Since Riak S2 mimics Amazon S3 API, that documentation could also be helpful:
[service][amazon-s3-service-docs],
[bucket][amazon-s3-bucket-docs],
[object][amazon-s3-object-docs],
[error][amazon-s3-error-docs].




### How To Use

Build and run the docker container.
The build script will create a user for you and store all information
required for connection to the `.docker.env.config` file.

```bash
$ ./run-docker.sh
```

Now we have Riak S2 initialized and started within container.
To build and start playing with the library, execute following commands in the shell:

```bash
$ make app shell
```

Here basic operations that you can perform. Refer to documentation to get to know more.

```erlang
%% Initializing connection to Riak S2.
{ok, Conf} = file:consult(".docker.env.config"),
{_, Opts} = lists:keyfind(user, 1, Conf),
{_, #{host := Host, port := Port}} = lists:keyfind(httpc_options, 1, Conf),
{ok, Pid} = gun:open(Host, Port, #{protocols => [http]}).

%% Creating a bucket, uploading an object and get it back
riaks2c_bucket:put(Pid, <<"test-bucket">>, #{}, Opts),
riaks2c_object:put(Pid, <<"test-bucket">>, <<"test_file">>, <<"content">>, <<"text/plain">>, #{}, Opts),
riaks2c_object:get(Pid, <<"test-bucket">>, <<"test_file">>, #{}, Opts).
%% <<"content">>

%% Listing buckets
riaks2c_bucket:list(Pid, #{}, Opts).
%% {'ListAllMyBucketsResult',[],
%%   {'CanonicalUser',[],
%%     <<"9897a3ea1c87262d4726b28b3e41c1e70bc9e20f54acdc12c5081e62f67b3323">>,
%%     <<"user">>},
%%   {'ListAllMyBucketsList',[],
%%     [{'ListAllMyBucketsEntry',[],<<"test-bucket">>,
%%       <<"2016-11-04T09:47:47.000Z">>}]}}

%% Listing objects
riaks2c_object:list(Pid, <<"test-bucket">>, #{}, Opts).
%% {'ListBucketResult',[],
%%   undefined,<<"test-bucket">>,[],[],
%%   undefined,1000,[],false,
%%   [{'ListEntry',[],
%%     <<"test_file">>,<<"2016-11-04T09:47:47.000Z">>,
%%     <<"\"9a0364b9e99bb480dd25e1f0284c8555\"">>,<<"7">>,
%%     <<"STANDARD">>,
%%     {'CanonicalUser',[],
%%       <<"9897a3ea1c87262d4726b28b"...>>,
%%       <<"user">>}}],
%%   undefined}

%% Cleaning up
riaks2c_object:remove(Pid, <<"test-bucket">>, <<"test_file">>, #{}, Opts),
riaks2c_bucket:remove(Pid, <<"test-bucket">>, #{}, Opts).
```



### License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
[travis]:https://travis-ci.org/manifest/riak-s2-erlang-client?branch=master
[travis-img]:https://secure.travis-ci.org/manifest/riak-s2-erlang-client.png
[riak-s2-docs]:http://docs.basho.com/riak/cs/2.1.1/references/apis/storage/#service-level-operations
[amazon-s3-service-docs]:http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceOps.html
[amazon-s3-bucket-docs]:http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketOps.html
[amazon-s3-object-docs]:http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectOps.html
[amazon-s3-error-docs]:http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#RESTErrorResponses
