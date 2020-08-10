# Copyright 2020 ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Tests for stream operations: ListStreams, DescribeStream, GetShardIterator,
# GetRecords.

import pytest
import time
import urllib.request

from botocore.exceptions import ClientError
from util import list_tables, test_table_name, create_test_table, random_string, freeze
from contextlib import contextmanager
from urllib.error import URLError
from boto3.dynamodb.types import TypeDeserializer

stream_types = [ 'OLD_IMAGE', 'NEW_IMAGE', 'KEYS_ONLY', 'NEW_AND_OLD_IMAGES']

def disable_stream(dynamodbstreams, table):
    while True:
        try:
            table.update(StreamSpecification={'StreamEnabled': False});
            while True:
                streams = dynamodbstreams.list_streams(TableName=table.name)
                if not streams.get('Streams'):
                    return
                # when running against real dynamodb, modifying stream 
                # state has a delay. need to wait for propagation. 
                print("Stream(s) lingering. Sleep 10s...")
                time.sleep(10)
        except ClientError as ce:
            # again, real dynamo has periods when state cannot yet
            # be modified. 
            if ce.response['Error']['Code'] == 'ResourceInUseException':
                print("Stream(s) in use. Sleep 10s...")
                time.sleep(10)
                continue
            raise
            
#
# Cannot use fixtures. Because real dynamodb cannot _remove_ a stream
# once created. It can only expire 24h later. So reusing test_table for 
# example works great for scylla/local testing, but once we run against
# actual aws instances, we get lingering streams, and worse: cannot 
# create new ones to replace it, because we have overlapping types etc. 
# 
# So we have to create and delete a table per test. And not run this 
# test to often against aws.  
@contextmanager
def create_stream_test_table(dynamodb, StreamViewType=None):
    spec = { 'StreamEnabled': False }
    if StreamViewType != None:
        spec = {'StreamEnabled': True, 'StreamViewType': StreamViewType}
    table = create_test_table(dynamodb, StreamSpecification=spec,
        KeySchema=[ { 'AttributeName': 'p', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c', 'KeyType': 'RANGE' }
        ],
        AttributeDefinitions=[
                    { 'AttributeName': 'p', 'AttributeType': 'S' },
                    { 'AttributeName': 'c', 'AttributeType': 'S' },
        ])
    yield table
    while True:
        try:
            table.delete()
            return
        except ClientError as ce:
            # if the table has a stream currently being created we cannot
            # delete the table immediately. Again, only with real dynamo
            if ce.response['Error']['Code'] == 'ResourceInUseException':
                print('Could not delete table yet. Sleeping 5s.')
                time.sleep(5)
                continue;
            raise

def wait_for_active_stream(dynamodbstreams, table, timeout=60):
    exp = time.process_time() + timeout
    while time.process_time() < exp:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        for stream in streams['Streams']:
            desc = dynamodbstreams.describe_stream(StreamArn=stream['StreamArn'])['StreamDescription']
            if not 'StreamStatus' in desc or desc.get('StreamStatus') == 'ENABLED':
                arn = stream['StreamArn']
                if arn != None:
                    return arn;
        # real dynamo takes some time until a stream is usable
        print("Stream not available. Sleep 5s...")
        time.sleep(5)
    assert False

# Local java dynamodb server version behaves differently from 
# the "real" one. Most importantly, it does not verify a number of 
# parameters, and consequently does not throw when called with borked 
# args. This will try to check if we are in fact running against 
# this test server, and if so, just raise the error here and be done 
# with it. All this just so we can run through the tests on 
# aws, scylla and local. 
def is_local_java(dynamodbstreams):
    # no good way to check, but local server runs on a Jetty, 
    # so check for that. 
    url = dynamodbstreams.meta.endpoint_url
    try: 
        urllib.request.urlopen(url)
    except URLError as e:
        return e.info()['Server'].startswith('Jetty')
    return False

def ensure_java_server(dynamodbstreams, error='ValidationException'):
    # no good way to check, but local server has a "shell" builtin, 
    # so check for that. 
    if is_local_java(dynamodbstreams):
        if error != None:
            raise ClientError({'Error': { 'Code' : error }}, '')
        return
    assert False

def test_list_streams_create(dynamodb, dynamodbstreams):
    for type in stream_types:
        with create_stream_test_table(dynamodb, StreamViewType=type) as table:
            wait_for_active_stream(dynamodbstreams, table)

def test_list_streams_alter(dynamodb, dynamodbstreams):
    for type in stream_types:
        with create_stream_test_table(dynamodb, StreamViewType=None) as table:
            res = table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': type});
            wait_for_active_stream(dynamodbstreams, table)

def test_list_streams_paged(dynamodb, dynamodbstreams):
    for type in stream_types:
        with create_stream_test_table(dynamodb, StreamViewType=type) as table1:
            with create_stream_test_table(dynamodb, StreamViewType=type) as table2:
                wait_for_active_stream(dynamodbstreams, table1)
                wait_for_active_stream(dynamodbstreams, table2)
                streams = dynamodbstreams.list_streams(Limit=1)
                assert streams
                assert streams.get('Streams')
                assert streams.get('LastEvaluatedStreamArn')
                tables = [ table1.name, table2.name ]
                while True:
                    for s in streams['Streams']:
                        name = s['TableName']
                        if name in tables: tables.remove(name)
                    if not tables:
                        break
                    streams = dynamodbstreams.list_streams(Limit=1, ExclusiveStartStreamArn=streams['LastEvaluatedStreamArn'])


@pytest.mark.skip(reason="Python driver validates Limit, so trying to test it is pointless")
def test_list_streams_zero_limit(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        with pytest.raises(ClientError, match='ValidationException'):
            wait_for_active_stream(dynamodbstreams, table)
            dynamodbstreams.list_streams(Limit=0)

def test_create_streams_wrong_type(dynamodb, dynamodbstreams, test_table):
    with pytest.raises(ClientError, match='ValidationException'):
        # should throw
        test_table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'Fisk'});
        # just in case the test fails, disable stream again
        test_table.update(StreamSpecification={'StreamEnabled': False});

def test_list_streams_empty(dynamodb, dynamodbstreams, test_table):
    streams = dynamodbstreams.list_streams(TableName=test_table.name)
    assert 'Streams' in streams
    assert not streams['Streams'] # empty

def test_list_streams_with_nonexistent_last_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        with pytest.raises(ClientError, match='ValidationException'):
            streams = dynamodbstreams.list_streams(TableName=table.name, ExclusiveStartStreamArn='kossaapaaasdafsdaasdasdasdasasdasfadfadfasdasdas')
            assert 'Streams' in streams
            assert not streams['Streams'] # empty
            # local java dynamodb does _not_ raise validation error for 
            # malformed stream arn here. verify 
            ensure_java_server(dynamodbstreams)

def test_describe_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        assert desc;
        assert desc.get('StreamDescription')
        assert desc['StreamDescription']['StreamArn'] == arn
        assert desc['StreamDescription']['StreamStatus'] != 'DISABLED'
        assert desc['StreamDescription']['StreamViewType'] == 'KEYS_ONLY'
        assert desc['StreamDescription']['TableName'] == table.name
        assert desc['StreamDescription'].get('Shards')
        assert desc['StreamDescription']['Shards'][0].get('ShardId')
        assert desc['StreamDescription']['Shards'][0].get('SequenceNumberRange')
        assert desc['StreamDescription']['Shards'][0]['SequenceNumberRange'].get('StartingSequenceNumber')

@pytest.mark.xfail(reason="alternator does not have creation time or label for streams")
def test_describe_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        assert desc;
        assert desc.get('StreamDescription')
        # note these are non-required attributes
        assert 'StreamLabel' in desc['StreamDescription']
        assert 'CreationRequestDateTime' in desc['StreamDescription']

def test_describe_nonexistent_stream(dynamodb, dynamodbstreams):
    with pytest.raises(ClientError, match='ResourceNotFoundException' if is_local_java(dynamodbstreams) else 'ValidationException'):
        streams = dynamodbstreams.describe_stream(StreamArn='sdfadfsdfnlfkajakfgjalksfgklasjklasdjfklasdfasdfgasf')

def test_describe_stream_with_nonexistent_last_shard(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        try:
            desc = dynamodbstreams.describe_stream(StreamArn=arn, ExclusiveStartShardId='zzzzzzzzzzzzzzzzzzzzzzzzsfasdgagadfadfgagkjsdfsfsdjfjks')
            assert not desc['StreamDescription']['Shards']
        except:
            # local java throws here. real does not. 
            ensure_java_server(dynamodbstreams, error=None)

def test_get_shard_iterator(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        desc = dynamodbstreams.describe_stream(StreamArn=arn)

        shard_id = desc['StreamDescription']['Shards'][0]['ShardId'];
        seq = desc['StreamDescription']['Shards'][0]['SequenceNumberRange']['StartingSequenceNumber'];

        for type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']: 
            iter = dynamodbstreams.get_shard_iterator(
                StreamArn=arn, ShardId=shard_id, ShardIteratorType=type, SequenceNumber=seq
            )
            assert iter.get('ShardIterator')

        for type in ['TRIM_HORIZON', 'LATEST']: 
            iter = dynamodbstreams.get_shard_iterator(
                StreamArn=arn, ShardId=shard_id, ShardIteratorType=type
            )
            assert iter.get('ShardIterator')

        for type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']: 
            # must have seq in these modes
            with pytest.raises(ClientError, match='ValidationException'):
                iter = dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId=shard_id, ShardIteratorType=type
                )

        for type in ['TRIM_HORIZON', 'LATEST']: 
            # should not set "seq" in these modes
            with pytest.raises(ClientError, match='ValidationException'):
                dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId=shard_id, ShardIteratorType=type, SequenceNumber=seq
                )

        # bad arn
        with pytest.raises(ClientError, match='ValidationException'):
            iter = dynamodbstreams.get_shard_iterator(
                StreamArn='sdfadsfsdfsdgdfsgsfdabadfbabdadsfsdfsdfsdfsdfsdfsdfdfdssdffbdfdf', ShardId=shard_id, ShardIteratorType=type, SequenceNumber=seq
            )
        # bad shard id  
        with pytest.raises(ClientError, match='ResourceNotFoundException'):
            dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId='semprinidfaasdasfsdvacsdcfsdsvsdvsdvsdvsdvsdv', 
                ShardIteratorType='LATEST'
                )
        # bad iter type
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, 
                ShardIteratorType='bulle', SequenceNumber=seq
                )
        # bad seq 
        with pytest.raises(ClientError, match='ValidationException'):
            dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, 
                ShardIteratorType='LATEST', SequenceNumber='sdfsafglldfngjdafnasdflgnaldklkafdsgklnasdlv'
                )

def test_get_shard_iterator_for_nonexistent_stream(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        arn = wait_for_active_stream(dynamodbstreams, table)
        desc = dynamodbstreams.describe_stream(StreamArn=arn)
        shards = desc['StreamDescription']['Shards']
        with pytest.raises(ClientError, match='ResourceNotFoundException' if is_local_java(dynamodbstreams) else 'ValidationException'):
            dynamodbstreams.get_shard_iterator(
                    StreamArn='sdfadfsddafgdafsgjnadflkgnalngalsdfnlkasnlkasdfasdfasf', ShardId=shards[0]['ShardId'], ShardIteratorType='LATEST'
                )

def test_get_shard_iterator_for_nonexistent_shard(dynamodb, dynamodbstreams):
    with create_stream_test_table(dynamodb, StreamViewType='KEYS_ONLY') as table:
        streams = dynamodbstreams.list_streams(TableName=table.name)
        arn = streams['Streams'][0]['StreamArn'];
        with pytest.raises(ClientError, match='ResourceNotFoundException'):
            dynamodbstreams.get_shard_iterator(
                    StreamArn=arn, ShardId='adfasdasdasdasdasdasdasdasdasasdasd', ShardIteratorType='LATEST'
                )

def test_get_records(dynamodb, dynamodbstreams):
    # TODO: add tests for storage/transactionable variations and global/local index
    with create_stream_test_table(dynamodb, StreamViewType='NEW_AND_OLD_IMAGES') as table:
        arn = wait_for_active_stream(dynamodbstreams, table)

        p = 'piglet'
        c = 'ninja'
        val = 'lucifers'
        val2 = 'flowers'
        table.put_item(Item={'p': p, 'c': c, 'a1': val, 'a2': val2})

        nval = 'semprini'
        nval2 = 'nudge'
        table.update_item(Key={'p': p, 'c': c}, 
            AttributeUpdates={ 'a1': {'Value': nval, 'Action': 'PUT'},
                'a2': {'Value': nval2, 'Action': 'PUT'}
            })

        has_insert = False

        # in truth, we should sleep already here, since at least scylla
        # will not be able to produce any stream content until 
        # ~30s after insert/update (confidence iterval)
        # but it is useful to see a working null-iteration as well, so 
        # lets go already.
        while True:
            desc = dynamodbstreams.describe_stream(StreamArn=arn)
            iterators = []

            while True:
                shards = desc['StreamDescription']['Shards']

                for shard in shards:
                    shard_id = shard['ShardId']
                    start = shard['SequenceNumberRange']['StartingSequenceNumber']
                    iter = dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='AT_SEQUENCE_NUMBER',SequenceNumber=start)['ShardIterator']
                    iterators.append(iter)

                last_shard = desc["StreamDescription"].get("LastEvaluatedShardId")
                if not last_shard:
                    break

                desc = dynamodbstreams.describe_stream(StreamArn=arn, ExclusiveStartShardId=last_shard)

            next_iterators = []
            while iterators:
                iter = iterators.pop(0)
                response = dynamodbstreams.get_records(ShardIterator=iter, Limit=1000)
                next = response['NextShardIterator']

                if next != '':
                    next_iterators.append(next)

                records = response.get('Records')
                # print("Query {} -> {}".format(iter, records))
                if records:
                    for record in records:
                        # print("Record: {}".format(record))
                        type = record['eventName']
                        dynamodb = record['dynamodb']
                        keys = dynamodb['Keys']
                        
                        assert keys.get('p')
                        assert keys.get('c')
                        assert keys['p'].get('S')
                        assert keys['p']['S'] == p
                        assert keys['c'].get('S')
                        assert keys['c']['S'] == c

                        if type == 'MODIFY' or type == 'INSERT':
                            assert dynamodb.get('NewImage')
                            newimage = dynamodb['NewImage'];
                            assert newimage.get('a1')
                            assert newimage.get('a2')

                        if type == 'INSERT' or (type == 'MODIFY' and not has_insert):
                            assert newimage['a1']['S'] == val
                            assert newimage['a2']['S'] == val2
                            has_insert = True
                            continue
                        if type == 'MODIFY':
                            assert has_insert
                            assert newimage['a1']['S'] == nval
                            assert newimage['a2']['S'] == nval2
                            assert dynamodb.get('OldImage')
                            oldimage = dynamodb['OldImage'];
                            assert oldimage.get('a1')
                            assert oldimage.get('a2')
                            assert oldimage['a1']['S'] == val
                            assert oldimage['a2']['S'] == val2
                            return
            print("Not done. Sleep 10s...")
            time.sleep(10)
            iterators = next_iterators

def test_get_records_nonexistent_iterator(dynamodbstreams):
    with pytest.raises(ClientError, match='ValidationException'):
        dynamodbstreams.get_records(ShardIterator='sdfsdfsgagaddafgagasgasgasdfasdfasdfasdfasdgasdasdgasdg', Limit=1000)

##############################################################################

# Fixtures for creating a table with a stream enabled with one of the allowed
# StreamViewType settings (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES).
# Unfortunately changing the StreamViewType setting of an existing stream is
# not allowed (see test_streams_change_type), and while removing and re-adding
# a stream is posssible, it is very slow. So we create four different fixtures
# with the four different StreamViewType settings for these four fixtures.
#
# It turns out that DynamoDB makes reusing the same table in different tests
# very difficult, because when we request a "LATEST" iterator we sometimes
# miss the immediately following write (this issue doesn't happen in
# ALternator, just in DynamoDB - presumably LATEST adds some time slack?)
# So all the fixtures we create below have scope="function", meaning that a
# separate table is created for each of the tests using these fixtures. This
# slows the tests down a bit, but not by much (about 0.05 seconds per test).
# It is still worthwhile to use a fixture rather than to create a table
# explicitly - it is convenient, safe (the table gets deleted automatically)
# and if in the future we can work around the DynamoDB problem, we can return
# these fixtures to session scope.

def create_table_ss(dynamodb, dynamodbstreams, type):
    table = create_test_table(dynamodb,
        KeySchema=[{ 'AttributeName': 'p', 'KeyType': 'HASH' }, { 'AttributeName': 'c', 'KeyType': 'RANGE' }],
        AttributeDefinitions=[{ 'AttributeName': 'p', 'AttributeType': 'S' }, { 'AttributeName': 'c', 'AttributeType': 'S' }],
        StreamSpecification={ 'StreamEnabled': True, 'StreamViewType': type })
    arn = wait_for_active_stream(dynamodbstreams, table, timeout=60)
    yield table, arn
    table.delete()

@pytest.fixture(scope="function")
def test_table_ss_keys_only(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'KEYS_ONLY')

@pytest.fixture(scope="function")
def test_table_ss_new_image(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'NEW_IMAGE')

@pytest.fixture(scope="function")
def test_table_ss_old_image(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'OLD_IMAGE')

@pytest.fixture(scope="function")
def test_table_ss_new_and_old_images(dynamodb, dynamodbstreams):
    yield from create_table_ss(dynamodb, dynamodbstreams, 'NEW_AND_OLD_IMAGES')

# Test that it is, sadly, not allowed to use UpdateTable on a table which
# already has a stream enabled to change that stream's StreamViewType.
# Currently, Alternator does allow this (see issue #6939), so the test is
# marked xfail.
@pytest.mark.xfail(reason="Alternator allows changing StreamViewType - see issue #6939")
def test_streams_change_type(test_table_ss_keys_only):
    table, arn = test_table_ss_keys_only
    with pytest.raises(ClientError, match='ValidationException.*already'):
        table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'OLD_IMAGE'});
        # If the above change succeeded (because of issue #6939), switch it back :-)
        table.update(StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'KEYS_ONLY'});

# Utility function for listing all the shards of the given stream arn.
# Implemented by multiple calls to DescribeStream, possibly several pages
# until all the shards are returned. The return of this function should be
# cached - it is potentially slow, and DynamoDB documentation even states
# DescribeStream may only be called at a maximum rate of 10 times per second.
# list_shards() only returns the shard IDs, not the information about the
# shards' sequence number range, which is also returned by DescribeStream.
def list_shards(dynamodbstreams, arn):
    # By default DescribeStream lists a limit of 100 shards. For faster
    # tests we reduced the number of shards in the testing setup to
    # 32 (16 vnodes x 2 cpus), see issue #6979, so to still exercise this
    # paging feature, lets use a limit of 10.
    limit = 10
    response = dynamodbstreams.describe_stream(StreamArn=arn, Limit=limit)['StreamDescription']
    assert len(response['Shards']) <= limit
    shards = [x['ShardId'] for x in response['Shards']]
    while 'LastEvaluatedShardId' in response:
        response = dynamodbstreams.describe_stream(StreamArn=arn, Limit=limit,
            ExclusiveStartShardId=response['LastEvaluatedShardId'])['StreamDescription']
        assert len(response['Shards']) <= limit
        shards.extend([x['ShardId'] for x in response['Shards']])
    print('Number of shards in stream: {}'.format(len(shards)))
    return shards

# Utility function for getting shard iterators starting at "LATEST" for
# all the shards of the given stream arn.
def latest_iterators(dynamodbstreams, arn):
    iterators = []
    for shard_id in list_shards(dynamodbstreams, arn):
        iterators.append(dynamodbstreams.get_shard_iterator(StreamArn=arn,
            ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator'])
    return iterators

# Utility function for fetching more content from the stream (given its
# array of iterators) into an "output" array. Call repeatedly to get more
# content - the function returns a new array of iterators which should be
# used to replace the input list of iterators.
# Note that the order of the updates is guaranteed for the same partition,
# but cannot be trusted for *different* partitions.
def fetch_more(dynamodbstreams, iterators, output):
    new_iterators = []
    for iter in iterators:
        response = dynamodbstreams.get_records(ShardIterator=iter)
        if 'NextShardIterator' in response:
            new_iterators.append(response['NextShardIterator'])
        output.extend(response['Records'])
    return new_iterators

# Utility function for comparing "output" as fetched by fetch_more(), to a list
# expected_events, each of which looks like:
#   [type, keys, old_image, new_image]
# where type is REMOVE, INSERT or MODIFY.
# The "mode" parameter specifies which StreamViewType mode (KEYS_ONLY,
# OLD_IMAGE, NEW_IMAGE, NEW_AND_OLD_IMAGES) was supposedly used to generate
# "output". This mode dictates what we can compare - e.g., in KEYS_ONLY mode
# the compare_events() function ignores the the old and new image in
# expected_events.
# compare_events() throws an exception immediately if it sees an unexpected
# event, but if some of the expected events are just missing in the "output",
# it only returns false - suggesting maybe the caller needs to try again
# later - maybe more output is coming.
# Note that the order of events is only guaranteed (and therefore compared)
# inside a single partition.
def compare_events(expected_events, output, mode):
    # The order of expected_events is only meaningful inside a partition, so
    # let's convert it into a map indexed by partition key.
    expected_events_map = {}
    for event in expected_events:
        expected_type, expected_key, expected_old_image, expected_new_image = event
        # For simplicity, we actually use the entire key, not just the partiton
        # key. We only lose a bit of testing power we didn't plan to test anyway
        # (that events for different items in the same partition are ordered).
        key = freeze(expected_key)
        if not key in expected_events_map:
            expected_events_map[key] = []
        expected_events_map[key].append(event)
    # Iterate over the events in output. An event for a certain key needs to
    # be the *first* remaining event for this key in expected_events_map (and
    # then we remove this matched even from expected_events_map)
    for event in output:
        # In DynamoDB, eventSource is 'aws:dynamodb'. We decided to set it to
        # a *different* value - 'scylladb:alternator'. Issue #6931.
        assert 'eventSource' in event
        # Alternator is missing "awsRegion", which makes little sense for it
        # (although maybe we should have provided the DC name). Issue #6931.
        #assert 'awsRegion' in event
        # Alternator is also missing the "eventVersion" entry. Issue #6931.
        #assert 'eventVersion' in event
        # Check that eventID appears, but can't check much on what it is.
        assert 'eventID' in event
        op = event['eventName']
        record = event['dynamodb']
        # record['Keys'] is "serialized" JSON, ({'S', 'thestring'}), so we
        # want to deserialize it to match our expected_events content.
        deserializer = TypeDeserializer()
        key = {x:deserializer.deserialize(y) for (x,y) in record['Keys'].items()}
        expected_type, expected_key, expected_old_image, expected_new_image = expected_events_map[freeze(key)].pop(0)
        if expected_type != '*': # hack to allow a caller to not test op, to bypass issue #6918.
            assert op == expected_type
        assert record['StreamViewType'] == mode
        # Check that all the expected members appear in the record, even if
        # we don't have anything to compare them to (TODO: we should probably
        # at least check they have proper format).
        assert 'ApproximateCreationDateTime' in record
        assert 'SequenceNumber' in record
        # Alternator doesn't set the SizeBytes member. Issue #6931.
        #assert 'SizeBytes' in record
        if mode == 'KEYS_ONLY':
            assert not 'NewImage' in record
            assert not 'OldImage' in record
        elif mode == 'NEW_IMAGE':
            assert not 'OldImage' in record
            if expected_new_image == None:
                assert not 'NewImage' in record
            else:
                new_image = {x:deserializer.deserialize(y) for (x,y) in record['NewImage'].items()}
                assert expected_new_image == new_image
        elif mode == 'OLD_IMAGE':
            assert not 'NewImage' in record
            if expected_old_image == None:
                assert not 'OldImage' in record
                pass
            else:
                old_image = {x:deserializer.deserialize(y) for (x,y) in record['OldImage'].items()}
                assert expected_old_image == old_image
        elif mode == 'NEW_AND_OLD_IMAGES':
            if expected_new_image == None:
                assert not 'NewImage' in record
            else:
                new_image = {x:deserializer.deserialize(y) for (x,y) in record['NewImage'].items()}
                assert expected_new_image == new_image
            if expected_old_image == None:
                assert not 'OldImage' in record
            else:
                old_image = {x:deserializer.deserialize(y) for (x,y) in record['OldImage'].items()}
                assert expected_old_image == old_image
        else:
            pytest.fail('cannot happen')
    # After the above loop, expected_events_map should remain empty arrays.
    # If it isn't, one of the expected events did not yet happen. Return False.
    for entry in expected_events_map.values():
        if len(entry) > 0:
            return False
    return True

# Convenience funtion used to implement several tests below. It runs a given
# function "updatefunc" which is supposed to do some updates to the table
# and also return an expected_events list. do_test() then fetches the streams
# data and compares it to the expected_events using compare_events().
def do_test(test_table_ss_stream, dynamodbstreams, updatefunc, mode, p = random_string(), c = random_string()):
    table, arn = test_table_ss_stream
    iterators = latest_iterators(dynamodbstreams, arn)
    expected_events = updatefunc(table, p, c)
    # Updating the stream is asynchronous. Moreover, it may even be delayed
    # artificially (see Alternator's alternator_streams_time_window_s config).
    # So if compare_events() reports the stream data is missing some of the
    # expected events (by returning False), we need to retry it for some time.
    # Note that compare_events() throws if the stream data contains *wrong*
    # (unexpected) data, so even failing tests usually finish reasonably
    # fast - depending on the alternator_streams_time_window_s parameter.
    # This is optimization is important to keep *failing* tests reasonably
    # fast and not have to wait until the following arbitrary timeout.
    timeout = time.time() + 20
    output = []
    while time.time() < timeout:
        iterators = fetch_more(dynamodbstreams, iterators, output)
        print("after fetch_more number expected_events={}, output={}".format(len(expected_events), len(output)))
        if compare_events(expected_events, output, mode):
            # success!
            return
        time.sleep(0.5)
    # If we're still here, the last compare_events returned false.
    pytest.fail('missing events in output: {}'.format(output))

# Test a single PutItem of a new item. Should result in a single INSERT
# event. Currently fails because in Alternator, PutItem - which generates a
# tombstone to *replace* an item - generates REMOVE+MODIFY (issue #6930).
@pytest.mark.xfail(reason="Currently fails - see issue #6930")
def test_streams_putitem_keys_only(test_table_ss_keys_only, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.put_item(Item={'p': p, 'c': c, 'x': 2})
        events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        return events
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates, 'KEYS_ONLY')

# Test a single UpdateItem. Should result in a single INSERT event.
# Currently fails because Alternator generates a MODIFY event even though
# this is a new item (issue #6918).
@pytest.mark.xfail(reason="Currently fails - see issue #6918")
def test_streams_updateitem_keys_only(test_table_ss_keys_only, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        return events
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates, 'KEYS_ONLY')

# This is exactly the same test as test_streams_updateitem_keys_only except
# we don't verify the type of even we find (MODIFY or INSERT). It allows us
# to have at least one good GetRecords test until solving issue #6918.
# When we do solve that issue, this test should be removed.
def test_streams_updateitem_keys_only_2(test_table_ss_keys_only, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        return events
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates, 'KEYS_ONLY')

# Test OLD_IMAGE using UpdateItem. Verify that the OLD_IMAGE indeed includes,
# as needed, the entire old item and not just the modified columns.
# Currently fails in Alternator because the item's key is missing in OldImage (#6935).
@pytest.mark.xfail(reason="Currently fails - see issue #6935")
def test_streams_updateitem_old_image(test_table_ss_old_image, dynamodbstreams):
    def do_updates(table, p, c):
        events = []
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 2})
        # We use here '*' instead of the really expected 'INSERT' to avoid
        # checking again the same Alternator bug already checked by
        # test_streams_updateitem_keys_only (issue #6918).
        # Note: The "None" as OldImage here tests that the OldImage should be
        # missing because the item didn't exist. This reproduces issue #6933.
        events.append(['*', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
        table.update_item(Key={'p': p, 'c': c},
            UpdateExpression='SET y = :val1', ExpressionAttributeValues={':val1': 3})
        events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2}, {'p': p, 'c': c, 'x': 2, 'y': 3}])
        return events
    do_test(test_table_ss_old_image, dynamodbstreams, do_updates, 'OLD_IMAGE')

# TODO: test OLD_IMAGE as in test_streams_updateitem_old_image, except one
# column is an LSI key. Because the LSI key is a real regular column in the
# base table, not a map member, the CDC's preimage will not include it.
# For more information, see https://github.com/scylladb/scylla/issues/6935#issuecomment-664847634

# TODO: test NEW_IMAGE and NEW_AND_OLD_IMAGES, similar to OLD_IMAGE above.
# In fact, it can be exactly the same code, sharing one do_update() function.
# Just make sure to keep three separate tests on the three separate tables -
# testing NEW_AND_OLD_IMAGES is not enough - OLD_IMAGE might have a bug,
# e.g., a missing column, that isn't missing in NEW_AND_OLD_IMAGES.

# Test that when a stream shard has no data to read, GetRecords returns an
# empty Records array - not a missing one. Reproduces issue #6926.
def test_streams_no_records(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    # Get just one shard - any shard - and its LATEST iterator. Because it's
    # LATEST, there will be no data to read from this iterator.
    shard_id = dynamodbstreams.describe_stream(StreamArn=arn, Limit=1)['StreamDescription']['Shards'][0]['ShardId']
    iter = dynamodbstreams.get_shard_iterator(StreamArn=arn, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']
    response = dynamodbstreams.get_records(ShardIterator=iter)
    assert 'NextShardIterator' in response
    assert 'Records' in response
    # We expect Records to be empty - there is no data at the LATEST iterator.
    assert response['Records'] == []

# Test that after fetching the last result from a shard, we don't get it
# yet again. Reproduces issue #6942.
def test_streams_last_result(test_table_ss_keys_only, dynamodbstreams):
    table, arn = test_table_ss_keys_only
    iterators = latest_iterators(dynamodbstreams, arn)
    # Do an UpdateItem operation that is expected to leave one event in the
    # stream.
    table.update_item(Key={'p': random_string(), 'c': random_string()},
        UpdateExpression='SET x = :val1', ExpressionAttributeValues={':val1': 5})
    # Eventually (we may need to retry this for a while), *one* of the
    # stream shards will return one event:
    timeout = time.time() + 15
    while time.time() < timeout:
        for iter in iterators:
            response = dynamodbstreams.get_records(ShardIterator=iter)
            if 'Records' in response and response['Records'] != []:
                # Found the shard with the data! Test that it only has
                # one event and that if we try to read again, we don't
                # get more data (this was issue #6942).
                assert len(response['Records']) == 1
                assert 'NextShardIterator' in response
                response = dynamodbstreams.get_records(ShardIterator=response['NextShardIterator'])
                assert response['Records'] == []
                return
        time.sleep(0.5)
    pytest.fail("timed out")

# Above we tested some specific operations in small tests aimed to reproduce
# a specific bug, in the following tests we do a all the different operations,
# PutItem, DeleteItem, BatchWriteItem and UpdateItem, and check the resulting
# stream for correctness.
# The following tests focus on mulitple operations on the *same* item. Those
# should appear in the stream in the correct order.
def do_updates_1(table, p, c):
    events = []
    # a first put_item appears as an INSERT event. Note also empty old_image.
    table.put_item(Item={'p': p, 'c': c, 'x': 2})
    events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'x': 2}])
    # a second put_item of the *same* key and same value, doesn't appear in the log at all!
    table.put_item(Item={'p': p, 'c': c, 'x': 2})
    # a second put_item of the *same* key and different value, appears as a MODIFY event
    table.put_item(Item={'p': p, 'c': c, 'y': 3})
    events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'x': 2}, {'p': p, 'c': c, 'y': 3}])
    # deleting an item appears as a REMOVE event. Note no new_image at all, but there is an old_image.
    table.delete_item(Key={'p': p, 'c': c})
    events.append(['REMOVE', {'p': p, 'c': c}, {'p': p, 'c': c, 'y': 3}, None])
    # deleting a non-existant item doesn't appear in the log at all.
    table.delete_item(Key={'p': p, 'c': c})
    # If update_item creates an item, the event is INSERT as well.
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET b = :val1',
        ExpressionAttributeValues={':val1': 4})
    events.append(['INSERT', {'p': p, 'c': c}, None, {'p': p, 'c': c, 'b': 4}])
    # If update_item modifies the item, note how old and new image includes both old and new columns
    table.update_item(Key={'p': p, 'c': c},
        UpdateExpression='SET x = :val1',
        ExpressionAttributeValues={':val1': 5})
    events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'b': 4}, {'p': p, 'c': c, 'b': 4, 'x': 5}])
    # TODO: incredibly, if we uncomment the "REMOVE b" update below, it will be
    # completely missing from the DynamoDB stream - the test continues to
    # pass even though we didn't add another expected event, and even though
    # the preimage in the following expected event includes this "b" we will
    # remove. I couldn't reproduce this apparant DynamoDB bug in a smaller test.
    #table.update_item(Key={'p': p, 'c': c}, UpdateExpression='REMOVE b')
    # Test BatchWriteItem as well. This modifies the item, so will be a MODIFY event.
    table.meta.client.batch_write_item(RequestItems = {table.name: [{'PutRequest': {'Item': {'p': p, 'c': c, 'x': 5}}}]})
    events.append(['MODIFY', {'p': p, 'c': c}, {'p': p, 'c': c, 'b': 4, 'x': 5}, {'p': p, 'c': c, 'x': 5}])
    return events

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_keys_only(test_table_ss_keys_only, dynamodbstreams):
    do_test(test_table_ss_keys_only, dynamodbstreams, do_updates_1, 'KEYS_ONLY')

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_new_image(test_table_ss_new_image, dynamodbstreams):
    do_test(test_table_ss_new_image, dynamodbstreams, do_updates_1, 'NEW_IMAGE')

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_old_image(test_table_ss_old_image, dynamodbstreams):
    do_test(test_table_ss_old_image, dynamodbstreams, do_updates_1, 'OLD_IMAGE')

@pytest.mark.xfail(reason="Currently fails - because of multiple issues listed above")
def test_streams_1_new_and_old_images(test_table_ss_new_and_old_images, dynamodbstreams):
    do_test(test_table_ss_new_and_old_images, dynamodbstreams, do_updates_1, 'NEW_AND_OLD_IMAGES')

# TODO: tests on multiple partitions
# TODO: write a test that disabling the stream and re-enabling it works, but
#   requires the user to wait for the first stream to become DISABLED before
#   creating the new one. Then ListStreams should return the two streams,
#   one DISABLED and one ENABLED? I'm not sure we want or can do this in
#   Alternator.
# TODO: Can we test shard ending, or shard splitting? (shard splitting
#   requires the user to - periodically or following shards ending - to call
#   DescribeStream again. We don't do this in any of our tests.
