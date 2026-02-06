# frozen_string_literal: true
require 'spec_helper'
require 'parallel_tests/rspec/queue_runner'

describe ParallelTests::RSpec::QueueRunner do
  let(:runner) { ParallelTests::RSpec::Runner }
  let(:options) { {} }

  def new_queue_runner(files, num_processes = 2, opts = options)
    ParallelTests::RSpec::QueueRunner.new(files, num_processes, opts, runner)
  end

  describe '#next_chunk' do
    around { |test| use_temporary_directory(&test) }

    it "returns files up to the target seconds based on runtime" do
      files = ['a_spec.rb', 'b_spec.rb', 'c_spec.rb', 'd_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        'a_spec.rb' => 3.0,
        'b_spec.rb' => 4.0,
        'c_spec.rb' => 5.0,
        'd_spec.rb' => 2.0
      )

      qr = new_queue_runner(files, 2)
      chunk = qr.next_chunk

      # sorted largest first: c(5), b(4), a(3), d(2)
      # first chunk grabs until >= 10s: c(5) + b(4) + a(3) = 12s
      expect(chunk).to eq(['c_spec.rb', 'b_spec.rb', 'a_spec.rb'])
    end

    it "returns remaining files in second chunk" do
      files = ['a_spec.rb', 'b_spec.rb', 'c_spec.rb', 'd_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        'a_spec.rb' => 3.0,
        'b_spec.rb' => 4.0,
        'c_spec.rb' => 5.0,
        'd_spec.rb' => 2.0
      )

      qr = new_queue_runner(files, 2)
      qr.next_chunk
      chunk2 = qr.next_chunk

      expect(chunk2).to eq(['d_spec.rb'])
    end

    it "returns empty array when queue is exhausted" do
      files = ['a_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return('a_spec.rb' => 1.0)

      qr = new_queue_runner(files, 2)
      qr.next_chunk
      expect(qr.next_chunk).to eq([])
    end

    it "always grabs at least one file even if it exceeds target seconds" do
      files = ['big_spec.rb']
      File.write('big_spec.rb', 'x')
      allow(runner).to receive(:runtimes).and_return('big_spec.rb' => 60.0)

      qr = new_queue_runner(files, 2)
      expect(qr.next_chunk).to eq(['big_spec.rb'])
    end

    it "falls back to filesize when no runtime data is available" do
      File.write('small_spec.rb', 'x' * 1000)
      File.write('big_spec.rb', 'x' * 200_000)
      allow(runner).to receive(:runtimes).and_raise(StandardError)

      qr = new_queue_runner(['small_spec.rb', 'big_spec.rb'], 2)

      # big file sorted first (200_000/10_000 = 20s > target)
      chunk = qr.next_chunk
      expect(chunk).to eq(['big_spec.rb'])
    end

    it "is thread-safe and distributes all files exactly once" do
      files = (1..20).map { |i| "spec_#{i}_spec.rb" }
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        files.each_with_object({}) { |f, h| h[f] = 1.0 }
      )

      qr = new_queue_runner(files, 4)
      all_chunks = []
      mutex = Mutex.new

      threads = 4.times.map do
        Thread.new do
          loop do
            chunk = qr.next_chunk
            break if chunk.empty?
            mutex.synchronize { all_chunks.concat(chunk) }
          end
        end
      end
      threads.each(&:join)

      expect(all_chunks.sort).to eq(files.sort)
    end
  end

  describe 'build_queue' do
    around { |test| use_temporary_directory(&test) }

    it "sorts files largest runtime first" do
      files = ['fast_spec.rb', 'slow_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        'fast_spec.rb' => 1.0,
        'slow_spec.rb' => 5.0
      )

      qr = new_queue_runner(files, 1)
      # slow file should come first
      chunk = qr.next_chunk
      expect(chunk.first).to eq('slow_spec.rb')
    end

    it "uses filesize for files missing from runtime log" do
      File.write('known_spec.rb', 'x')
      File.write('unknown_spec.rb', 'x' * 200_000)
      allow(runner).to receive(:runtimes).and_return('known_spec.rb' => 1.0)

      qr = new_queue_runner(['known_spec.rb', 'unknown_spec.rb'], 1)
      # unknown_spec has 200_000/10_000 = 20s estimated, so it comes first
      chunk = qr.next_chunk
      expect(chunk.first).to eq('unknown_spec.rb')
    end
  end

  describe 'merge_results' do
    around { |test| use_temporary_directory(&test) }

    # Use runtimes > CHUNK_TARGET_SECONDS so each file gets its own chunk
    it "combines stdout from multiple chunks" do
      files = ['a_spec.rb', 'b_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        'a_spec.rb' => 15.0,
        'b_spec.rb' => 15.0
      )

      call_count = 0
      allow(runner).to receive(:run_tests) do |_files, _pn, _np, _opts|
        call_count += 1
        { stdout: "output#{call_count}\n", exit_status: 0, command: ["cmd#{call_count}"], seed: nil, env: {} }
      end

      qr = new_queue_runner(files, 1)
      result = qr.send(:run_worker, 0)

      expect(result[:stdout]).to include('output1')
      expect(result[:stdout]).to include('output2')
    end

    it "takes the maximum exit status" do
      files = ['a_spec.rb', 'b_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        'a_spec.rb' => 15.0,
        'b_spec.rb' => 15.0
      )

      results = [
        { stdout: 'ok', exit_status: 0, command: ['cmd1'], seed: nil, env: {} },
        { stdout: 'fail', exit_status: 1, command: ['cmd2'], seed: nil, env: {} }
      ]
      call_count = 0
      allow(runner).to receive(:run_tests) do
        call_count += 1
        results[call_count - 1]
      end

      qr = new_queue_runner(files, 1)
      result = qr.send(:run_worker, 0)

      expect(result[:exit_status]).to eq(1)
    end

    it "unions commands from all chunks" do
      files = ['a_spec.rb', 'b_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        'a_spec.rb' => 15.0,
        'b_spec.rb' => 15.0
      )

      call_count = 0
      allow(runner).to receive(:run_tests) do
        call_count += 1
        { stdout: '', exit_status: 0, command: ["rspec", "file#{call_count}"], seed: nil, env: {} }
      end

      qr = new_queue_runner(files, 1)
      result = qr.send(:run_worker, 0)

      expect(result[:command]).to include('rspec')
      expect(result[:command]).to include('file1')
      expect(result[:command]).to include('file2')
    end
  end

  describe 'run_worker' do
    around { |test| use_temporary_directory(&test) }

    it "returns empty result when no work is available" do
      allow(runner).to receive(:runtimes).and_return({})

      qr = new_queue_runner([], 2)
      result = qr.send(:run_worker, 0)

      expect(result[:stdout]).to eq('')
      expect(result[:exit_status]).to eq(0)
      expect(result[:command]).to eq([])
    end

    it "stops pulling chunks on fail_fast when a chunk fails" do
      files = ['a_spec.rb', 'b_spec.rb', 'c_spec.rb']
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        'a_spec.rb' => 1.0,
        'b_spec.rb' => 1.0,
        'c_spec.rb' => 1.0
      )

      allow(runner).to receive(:run_tests).and_return(
        { stdout: 'fail', exit_status: 1, command: ['cmd'], seed: nil, env: {} }
      )

      qr = new_queue_runner(files, 1, fail_fast: true)
      result = qr.send(:run_worker, 0)

      # should have only called run_tests once (all 3 files in one chunk since 3s < 10s)
      # and stopped after the failure
      expect(runner).to have_received(:run_tests).once
      expect(result[:exit_status]).to eq(1)
    end
  end

  describe '#run' do
    around { |test| use_temporary_directory(&test) }

    it "distributes work across multiple workers" do
      files = (1..20).map { |i| "spec_#{i}_spec.rb" }
      files.each { |f| File.write(f, 'x') }
      allow(runner).to receive(:runtimes).and_return(
        files.each_with_object({}) { |f, h| h[f] = 1.0 }
      )

      files_run_by_worker = Hash.new { |h, k| h[k] = [] }
      allow(runner).to receive(:run_tests) do |test_files, process_number, _np, _opts|
        files_run_by_worker[process_number].concat(test_files)
        { stdout: "#{test_files.size} examples, 0 failures\n", exit_status: 0, command: ['rspec', *test_files], seed: nil, env: {} }
      end

      cli = ParallelTests::CLI.new
      qr = new_queue_runner(files, 2)
      results = qr.run(cli)

      # all files were run
      all_files = files_run_by_worker.values.flatten
      expect(all_files.sort).to eq(files.sort)

      # results from both workers
      expect(results.size).to eq(2)
      results.each { |r| expect(r[:exit_status]).to eq(0) }
    end
  end
end
