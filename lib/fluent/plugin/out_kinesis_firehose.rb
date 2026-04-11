#
# Copyright 2014-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

require 'fluent/plugin/kinesis'

module Fluent
  module Plugin
    class KinesisFirehoseOutput < KinesisOutput
      Fluent::Plugin.register_output('kinesis_firehose', self)

      RequestType = :firehose
      BatchRequestLimitCount = 500
      BatchRequestLimitSize  = 4 * 1024 * 1024
      include KinesisHelper::API::BatchRequest

      config_param :delivery_stream_name, :string
      config_param :append_new_line,      :bool, default: true
      config_param :aggregated_record_size, :integer, default: 0

      def configure(conf)
        super
        if @append_new_line
          org_data_formatter = @data_formatter
          @data_formatter = ->(tag, time, record) {
            org_data_formatter.call(tag, time, record).chomp + "\n"
          }
        end
        if @aggregated_record_size > @max_record_size
          raise Fluent::ConfigError, "aggregated_record_size (#{@aggregated_record_size}) can't be greater than max_record_size (#{@max_record_size})."
        end
      end

      def format(tag, time, record)
        format_for_api do
          [@data_formatter.call(tag, time, record)]
        end
      end

      def write(chunk)
        delivery_stream_name = extract_placeholders(@delivery_stream_name, chunk)
        if @aggregated_record_size > 0
          write_records_batch_aggregated(chunk, delivery_stream_name) do |batch|
            records = batch.map{|(data)|
              { data: data }
            }
            client.put_record_batch(
              delivery_stream_name: delivery_stream_name,
              records: records,
            )
          end
        else
          write_records_batch(chunk, delivery_stream_name) do |batch|
            records = batch.map{|(data)|
              { data: data }
            }
            client.put_record_batch(
              delivery_stream_name: delivery_stream_name,
              records: records,
            )
          end
        end
      end

      private

      def write_records_batch_aggregated(chunk, stream_name, &block)
        unique_id = chunk.dump_unique_id_hex(chunk.unique_id)
        records = chunk.to_enum(:msgpack_each)
        aggregated = aggregate_records(records)
        split_to_batches(aggregated) do |batch, size|
          log.debug(sprintf "%s: Write chunk %s / %3d records / %4d KB", stream_name, unique_id, batch.size, size/1024)
          batch_request_with_retry(batch, &block)
          log.debug(sprintf "%s: Finish writing chunk", stream_name)
        end
      end

      def aggregate_records(records)
        Enumerator.new do |y|
          current = nil
          current_size = 0
          records.each do |record|
            data = record.first
            data_size = data.bytesize
            if current.nil?
              current = data
              current_size = data_size
            elsif current_size + data_size <= @aggregated_record_size
              current = current + data
              current_size += data_size
            else
              y.yield [current]
              current = data
              current_size = data_size
            end
          end
          y.yield [current] unless current.nil?
        end
      end
    end
  end
end
