#
# Copyright 2014-2026 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file.
# This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

require_relative 'aws_emulator_helper'
require 'fluent/plugin/out_kinesis_firehose'
require 'aws-sdk-firehose'
require 'aws-sdk-iam'
require 'aws-sdk-s3'
require 'json'

class KinesisFirehoseOutputAwsEmulatorTest < Test::Unit::TestCase
  include AwsEmulatorHelper

  def create_driver(conf)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::KinesisFirehoseOutput).configure(conf)
  end

  def firehose
    @firehose ||= Aws::Firehose::Client.new
  end

  def iam
    @iam ||= Aws::IAM::Client.new
  end

  def s3
    @s3 ||= Aws::S3::Client.new
  end

  # Create a DirectPut delivery stream delivering to a new S3 bucket without buffering delay
  def create_delivery_stream
    name = unique_name('fluent-plugin-kinesis-test')
    @bucket = name
    s3.create_bucket(bucket: @bucket)
    @role = name
    role_arn = iam.create_role(
      role_name: @role,
      assume_role_policy_document: {
        Version: '2012-10-17',
        Statement: [{ Effect: 'Allow', Principal: { Service: 'firehose.amazonaws.com' }, Action: 'sts:AssumeRole' }],
      }.to_json,
    ).role.arn
    iam.put_role_policy(
      role_name: @role,
      policy_name: 's3',
      policy_document: {
        Version: '2012-10-17',
        Statement: [{ Effect: 'Allow', Action: 's3:*', Resource: ["arn:aws:s3:::#{@bucket}", "arn:aws:s3:::#{@bucket}/*"] }],
      }.to_json,
    )
    @delivery_stream = name
    firehose.create_delivery_stream(
      delivery_stream_name: @delivery_stream,
      delivery_stream_type: 'DirectPut',
      extended_s3_destination_configuration: {
        role_arn: role_arn,
        bucket_arn: "arn:aws:s3:::#{@bucket}",
        buffering_hints: { size_in_m_bs: 1, interval_in_seconds: 0 },
      },
    )
    wait_until("delivery stream #{name} to be ACTIVE") do
      firehose.describe_delivery_stream(delivery_stream_name: name).delivery_stream_description.delivery_stream_status == 'ACTIVE'
    end
    name
  end

  def teardown
    if @delivery_stream
      firehose.delete_delivery_stream(delivery_stream_name: @delivery_stream, allow_force_delete: true) rescue nil
    end
    if @bucket
      s3.list_objects_v2(bucket: @bucket).contents.each { |o| s3.delete_object(bucket: @bucket, key: o.key) } rescue nil
      s3.delete_bucket(bucket: @bucket) rescue nil
    end
    if @role
      iam.delete_role_policy(role_name: @role, policy_name: 's3') rescue nil
      iam.delete_role(role_name: @role) rescue nil
    end
    super
  end

  # Read all objects delivered to the bucket, in the order of their keys
  def read_delivered_data(expected_bytesize)
    data = nil
    wait_until("#{expected_bytesize} bytes delivered to bucket #{@bucket}") do
      keys = s3.list_objects_v2(bucket: @bucket).contents.map(&:key).sort
      data = keys.map { |key| s3.get_object(bucket: @bucket, key: key).body.read }.join
      data.bytesize >= expected_bytesize
    end
    data
  end

  def test_put_record_batch_delivered_to_s3
    delivery_stream_name = create_delivery_stream
    records = (1..3).map { |i| { 'id' => i, 'message' => "hello #{i}" } }
    d = create_driver("delivery_stream_name #{delivery_stream_name}")
    requests = record_requests(d.instance.client, :put_record_batch)
    driver_run(d, records)
    assert_no_error_logs(d)
    assert_equal [records.size], requests.map { |r| r[:records].size }

    expected = records.map { |r| "#{r.to_json}\n" }.join
    assert_equal expected, read_delivered_data(expected.bytesize)
  end

  def test_put_record_batch_with_data_key_without_append_new_line
    delivery_stream_name = create_delivery_stream
    records = (1..3).map { |i| { 'data' => "raw data #{i};", 'other' => i } }
    d = create_driver(%[
      delivery_stream_name #{delivery_stream_name}
      data_key data
      append_new_line false
    ])
    driver_run(d, records)
    assert_no_error_logs(d)

    expected = records.map { |r| r['data'] }.join
    assert_equal expected, read_delivered_data(expected.bytesize)
  end

  def test_put_record_batch_with_aggregated_record_size
    delivery_stream_name = create_delivery_stream
    records = (1..10).map { |i| { 'id' => i } }
    d = create_driver(%[
      delivery_stream_name #{delivery_stream_name}
      aggregated_record_size 30
    ])
    requests = record_requests(d.instance.client, :put_record_batch)
    driver_run(d, records)
    assert_no_error_logs(d)
    # Each formatted record ({"id":N}\n) is 9 or 10 bytes, so up to 3 records are aggregated into one
    sent = requests.flat_map { |r| r[:records].map { |record| record[:data] } }
    assert_equal 4, sent.size
    sent.each { |data| assert_operator data.bytesize, :<=, 30 }

    expected = records.map { |r| "#{r.to_json}\n" }.join
    assert_equal expected, sent.join
    assert_equal expected, read_delivered_data(expected.bytesize)
  end
end
