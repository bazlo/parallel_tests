# frozen_string_literal: true
require 'parallel_tests/rspec/runner'

module ParallelTests
  module RSpec
    class QueueRunner
      CHUNK_TARGET_SECONDS = 10.0

      def initialize(test_files, num_processes, options, runner)
        @runner = runner
        @num_processes = num_processes
        @options = options
        @mutex = Mutex.new
        @queue = build_queue(test_files, options)
      end

      def run(cli)
        items = (0...@num_processes).to_a
        cli.send(:execute_in_parallel, items, @num_processes, @options) do |_item, index|
          run_worker(index)
        end
      end

      def next_chunk
        @mutex.synchronize do
          chunk = []
          chunk_time = 0.0
          while @queue.any? && chunk_time < CHUNK_TARGET_SECONDS
            file, time = @queue.shift
            chunk << file
            chunk_time += time
          end
          chunk
        end
      end

      private

      def run_worker(process_number)
        combined_result = nil
        loop do
          chunk = next_chunk
          break if chunk.empty?

          result = @runner.run_tests(chunk, process_number, @num_processes, @options)

          combined_result = if combined_result.nil?
            result
          else
            merge_results(combined_result, result)
          end

          break if @options[:fail_fast] && result[:exit_status] != 0
        end
        combined_result || empty_result
      end

      def merge_results(base, addition)
        {
          stdout: base[:stdout].to_s + addition[:stdout].to_s,
          exit_status: [base[:exit_status], addition[:exit_status]].max,
          command: Array(base[:command]) | Array(addition[:command]),
          seed: addition[:seed] || base[:seed],
          env: addition[:env] || base[:env]
        }
      end

      def empty_result
        { stdout: '', exit_status: 0, command: [], seed: nil, env: nil }
      end

      def build_queue(test_files, options)
        runtimes = load_runtimes(test_files, options)
        files_with_time = test_files.map do |file|
          [file, runtimes[file] || estimate_by_filesize(file)]
        end
        files_with_time.sort_by { |_file, time| -time }
      end

      def load_runtimes(test_files, options)
        @runner.send(:runtimes, test_files, options)
      rescue StandardError
        {}
      end

      def estimate_by_filesize(file)
        [File.stat(file).size / 10_000.0, 0.1].max
      rescue StandardError
        1.0
      end
    end
  end
end
