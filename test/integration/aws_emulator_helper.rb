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

require_relative '../helper'
require 'securerandom'
require 'aws-sdk-kinesis'

# Helpers for integration tests against AWS Emulator.
# https://aws.amazon.com/products/developer-tools/emulator/
#
# These tests run only when AWS_EMULATOR=true.
# Point the AWS SDK to a running emulator with environment variables:
# AWS_ENDPOINT_URL, AWS_SHARED_CREDENTIALS_FILE, AWS_CONFIG_FILE, AWS_CA_BUNDLE and AWS_REGION.
# See the aws-emulator job in .github/workflows/test.yml for an example.
module AwsEmulatorHelper
  ENABLED = ENV['AWS_EMULATOR'] == 'true'

  # Unit tests with DummyServer overwrite and delete some of these variables.
  # Keep the values given at load time and restore them in each test.
  ENV_KEYS = %w[
    AWS_ENDPOINT_URL AWS_SHARED_CREDENTIALS_FILE AWS_CONFIG_FILE AWS_CA_BUNDLE
    AWS_EC2_METADATA_DISABLED AWS_REGION
  ].freeze
  CREDENTIAL_ENV_KEYS = %w[
    AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE
  ].freeze
  EMULATOR_ENV = ENV.to_h.select { |k, _| ENV_KEYS.include?(k) }.freeze

  WAIT_TIMEOUT = 60

  def setup
    omit('Set AWS_EMULATOR=true to run integration tests with AWS Emulator') unless ENABLED
    # Never create resources in real AWS accounts
    flunk('Set AWS_ENDPOINT_URL to the endpoint of AWS Emulator') unless EMULATOR_ENV['AWS_ENDPOINT_URL']
    @original_env = ENV.to_h.select { |k, _| (ENV_KEYS + CREDENTIAL_ENV_KEYS).include?(k) }
    CREDENTIAL_ENV_KEYS.each { |k| ENV.delete(k) }
    ENV.update(EMULATOR_ENV)
    # Never fall back to instance metadata if the emulator credentials are missing
    ENV['AWS_EC2_METADATA_DISABLED'] = 'true'
    # Reload shared config, which may have been cached under FakeFS by unit tests
    Aws.shared_config.fresh
    Fluent::Test.setup
  end

  def teardown
    return unless @original_env
    (ENV_KEYS + CREDENTIAL_ENV_KEYS).each { |k| ENV.delete(k) }
    ENV.update(@original_env)
  end

  def unique_name(prefix)
    "#{prefix}-#{Time.now.strftime('%Y%m%d%H%M%S')}-#{SecureRandom.hex(4)}"
  end

  def wait_until(description, timeout: WAIT_TIMEOUT, interval: 0.5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      result = yield
      return result if result
      if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
        flunk("Timed out after #{timeout} seconds waiting for #{description}")
      end
      sleep interval
    end
  end

  # Wrap an API method of the client to record the parameters of requests sent to the emulator
  def record_requests(client, operation)
    requests = []
    original = client.method(operation)
    client.define_singleton_method(operation) do |params = {}, *args, &block|
      requests << params
      original.call(params, *args, &block)
    end
    requests
  end

  def assert_no_error_logs(driver)
    assert_equal [], driver.logs.grep(/\[(error|fatal)\]/)
  end

  module Kinesis
    include AwsEmulatorHelper

    def kinesis
      @kinesis ||= Aws::Kinesis::Client.new
    end

    def create_stream(shard_count: 1)
      name = unique_name('fluent-plugin-kinesis-test')
      kinesis.create_stream(stream_name: name, shard_count: shard_count)
      (@streams ||= []) << name
      wait_until("stream #{name} to be ACTIVE") do
        kinesis.describe_stream_summary(stream_name: name).stream_description_summary.stream_status == 'ACTIVE'
      end
      name
    end

    def teardown
      (@streams || []).each do |name|
        begin
          kinesis.delete_stream(stream_name: name, enforce_consumer_deletion: true)
        rescue Aws::Kinesis::Errors::ResourceNotFoundException
        end
      end
      super
    end

    # Read all records in the stream from TRIM_HORIZON of each shard
    def read_records(stream_name, expected_count)
      shard_ids = kinesis.list_shards(stream_name: stream_name).shards.map(&:shard_id)
      iterators = shard_ids.map do |shard_id|
        kinesis.get_shard_iterator(
          stream_name: stream_name,
          shard_id: shard_id,
          shard_iterator_type: 'TRIM_HORIZON',
        ).shard_iterator
      end
      records = []
      wait_until("#{expected_count} records in stream #{stream_name}") do
        iterators = iterators.map do |iterator|
          next nil if iterator.nil?
          res = kinesis.get_records(shard_iterator: iterator)
          records.concat(res.records)
          res.next_shard_iterator
        end
        records.size >= expected_count
      end
      records
    end
  end
end
