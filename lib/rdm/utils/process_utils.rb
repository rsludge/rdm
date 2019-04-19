module Rdm
  module Utils
  end
end

module Rdm::Utils::ProcessUtils
  class WorkerProcess
    attr_accessor :name

    def initialize(name, log_file=nil)
      @name     = name
      @log_file = log_file
      @state    = :free
    end

    def set_busy
      @state = :busy
    end

    def execute_command(command)
      @child_pid = Process.fork do
        system("#{command} >> #{@log_file}")
      end

      Process.waitpid(@child_pid)
      @state = :free
    end

    def free?
      @state == :free
    end
  end

  class ParallelWorm
    SLEEP_DELAY = 0.1

    def execute_commands(commands, proccess_count)
      workers = []
      threads = []

      proccess_count.times{|n| workers << WorkerProcess.new("worker #{n}", "work_#{n}_log")}

      while(commands.size > 0) do
        command  = commands.shift

        worker = nil

        loop do
          worker = workers.detect(&:free?)
          break if worker
          sleep(SLEEP_DELAY)
        end

        # puts "execute command #{command} in worker #{worker.name}"
        worker.set_busy
        threads << Thread.new(worker, command) do |thread_worker, thread_command|
          thread_worker.execute_command(thread_command)
        end

      end

      threads.each(&:join)
    end
  end
end