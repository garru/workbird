require 'thread'
require 'logger'
require 'socket'
require 'rubygems'
module Workbird
  class Server
    attr_accessor :processor, :start_id, :end_id, :workers,
                  :timeout, :num_workers, :chunk_size,
                  :pid_file, :worker_queue, :busy_workers,:logger, :ranges,
                  :num_ranges, :busy_workers_mutex, :max_jobs, :state_file, :retry_queue

    DEFAULT_TIMEOUT = 30
    DEFAULT_PIDFILE = '/tmp/temporary_worker.pid'
    DEFAULT_STATE_FILE = '/tmp/temporay_worker.save'
    DEFAULT_WORKER_COUNT = 4
    SIG_QUEUE = []
    [:QUIT, :INT, :TERM, :CHLD].each { |s| trap(s) { SIG_QUEUE << s} }

    def initialize(proc, options = {})
      self.logger = Logger.new(options[:logger] || $stderr)
      self.logger.level = Logger::ERROR
      self.processor = proc
      self.start_id = options[:start_id]
      self.end_id = options[:end_id]
      self.chunk_size = options[:chunk_size] || 1
      self.state_file = options[:state_file] || DEFAULT_STATE_FILE
      self.timeout = options[:timeout] || DEFAULT_TIMEOUT
      self.pid_file = options[:pid] || DEFAULT_PIDFILE
      self.num_workers = options[:num_workers] || DEFAULT_WORKER_COUNT
      self.num_ranges = options[:ranges] || 2 #this could help improve eta
      setup_ranges
      self.retry_queue = RetryQueue.new(10)
      self.worker_queue = Queue.new
      self.busy_workers = []
      self.busy_workers_mutex = Mutex.new
      self.workers = {}
      self.max_jobs = 10
    end

    def setup_ranges
      id_slice = (self.end_id - self.start_id)/self.num_ranges
      current_slice = self.start_id
      self.ranges = []
      (self.num_ranges - 1).times do |x|
        self.ranges << Range.new(current_slice, current_slice + id_slice)
        current_slice = current_slice + id_slice
      end
      self.ranges << Range.new(current_slice, self.end_id)
    end

    def start
      begin
        setup_workers
        check_worker_loop
        self.logger.info("master ready")
        interactive_loop
        run_loop
      rescue => e
        puts e.message
        puts e.backtrace
      end
    end

    def run_loop
      start_time = Time.now
      jobs_left = true
      begin
        while jobs_left || SIG_QUEUE.size > 0
          jobs_left = false
          self.ranges.each do |x|
            empty_sig_queue unless SIG_QUEUE.empty?
            if chunk = self.retry_queue.take
              jobs_left = true
              self.send_job(chunk)
            end
            if chunk = x.take(self.chunk_size)
              jobs_left = true
              self.send_job(chunk)
            end
          end
        end
      rescue => e
        puts e.message
      end
      stop
      self.logger.info("Job completed in #{Time.now.to_f - start_time.to_f} seconds")
    end

    def empty_sig_queue
      SIG_QUEUE.each do |x|
        case x
        when :INT, :QUIT, :TERM
          stop
        end
      end
    end

    def stop
      time = Time.now
      while (Time.now.to_i - time.to_i) < self.timeout && busy_workers.size > 0
        sleep(0.1)
      end
      save_state
      kill_workers
      exit!
    end

    def kill_workers
      puts self.workers.inspect
      self.workers.each do |pid, worker|
        Process.kill(:KILL, pid)
      end
    end

    def send_job(chunk)
      worker_pid = self.next_worker_pid
      self.busy_workers_mutex.synchronize do
        self.busy_workers << worker_pid
      end
      self.logger.info("Sending job to #{worker_pid} #{chunk.size}")
      self.workers[worker_pid].process(chunk)
    end

    def check_worker_loop
      Thread.new do
        loop do
          begin
            sleep(1) if self.busy_workers.size == 0
            self.busy_workers.each do |x|
              self.logger.info("worker status #{workers[x].status} #{workers[x].current_job}")
              case workers[x].status
              when :success
                self.busy_workers_mutex.synchronize do
                  self.busy_workers.delete(x)
                end
                self.worker_queue << x
              when :failure

                self.busy_workers_mutex.synchronize do
                  self.busy_workers.delete(x)
                end
                self.worker_queue << x
              else
                if workers[x].processing_time > self.timeout
                  self.retry_queue << workers[x].current_job
                  reep_worker(x)
                  spawn_worker
                end
              end
            end
            sleep(0.01)
          rescue  => e
            puts e.backtrace
          end
        end
      end
    end

    def check_workers
      if self.workers.size < self.num_workers
        (self.num_workers - self.workers.keys.size).times{ spawn_worker }
      end
    end

    def save_state
      state = Marshal.dump([self.ranges, retry_queue])
      d = File.open(self.state_file, "w")
      d.puts(state)
      d.close
    end

    def setup_workers
      self.num_workers.times do |x|
        spawn_worker
      end
    end

    def spawn_worker
      c_rd, p_wr = IO.pipe
      p_rd, c_wr = IO.pipe
      if pid = fork
        self.logger.info("Spawned Worker #{pid}")
        w = create_worker(Worker::Parent.new(p_rd, p_wr, pid))
      else
        s = Worker::Child.new(c_rd, c_wr, self.logger, self.processor)
        s.job_listener
      end
      pid
    end

    def reep_worker(x)
      self.busy_workers_mutex.synchronize do
        self.busy_workers.delete(x)
      end
      worker = self.workers.delete(x)
      Process.kill(:KILL, worker.pid)
    end

    def create_worker(worker)
      self.workers[worker.pid] = worker
      self.worker_queue << worker.pid
    end

    def next_worker_pid
      worker = self.worker_queue.pop
      if self.workers[worker].num_jobs > self.max_jobs
        reep_worker(worker)
        worker = spawn_worker
      end
      worker
    end

    def interactive_loop
      last_command = nil
      Thread.new do
        begin
          puts "Workbird: type help for available commands"
          puts self.status
          loop do
            STDOUT.write " => "
            STDOUT.flush
            cmd = STDIN.readline
            cmd = cmd == "\n" ? last_command : cmd.chomp
            response = case cmd
            when "add"
              add_worker
            when "remove"
              remove_worker
            when "status"
              status
            when "help"
              help_commands
            when "quit"
              quit
            else
              "unknown command"
            end
            STDOUT.puts([response, "\n"].join("\n"))
            last_command = cmd
          end
        rescue => e
          puts e.message
          puts e.backtrace
        end
      end
    end

    def quit
      SIG_QUEUE << :QUIT
    end

    def progress
      self.ranges.inject(0){|sum, k| sum += k.progress}
    end

    def eta
      self.ranges.inject(0){|sum , k| sum += k.eta}
    end

    def help_commands
      ["add", "remove", "status"].join("\n")
    end

    def banner
    end

    def add_worker
      self.num_workers += 1
      spawn_worker
      "worker added (total # workers: #{self.num_workers})"
    end

    def remove_worker
      self.num_workers -= 1
      worker_id = self.worker_queue.pop
      reep_worker(worker_id)
      "worker removed (total # workers: #{self.num_workers})"
    end

    def status
      "Progress: #{progress} ETA: #{eta} Workers: #{num_workers}"
    end
  end
end