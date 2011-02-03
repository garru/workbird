module Workbird
  module Worker
    class Parent
      attr_accessor :current_job, :job_start_time, :status, :pid, :num_jobs

      def initialize(read_io, write_io, pid)
        @read_io = read_io
        @write_io = write_io
        @pid = pid
        @num_jobs = 0
      end

      def process(chunks)
        # puts "Sending job #{@pid} #{chunks}"
        self.current_job = chunks
        self.job_start_time = Time.now
        self.status = :in_process
        Thread.new do
          data = Marshal.dump(chunks)
          @write_io.puts(data.size)
          @write_io.write(data)
          ready, = IO.select([@read_io], nil, nil)
          if ready
            result = @read_io.gets
            if result == "s\n"
              self.status = :success
            else
              self.status = :failure
            end
          end
          @num_jobs += 1
        end
      end

      def processing_time
        Time.now.to_f - self.job_start_time.to_f
      end
    end

    class Child
      def initialize(read_io, write_io, logger, proc)
        @read_io = read_io
        @write_io = write_io
        @proc = proc
        @logger = logger
      end

      def job_listener
        loop do
          begin
            ready, = IO.select([@read_io], nil, nil)
            if ready
              job_size = @read_io.gets.to_i
              job = @read_io.read(job_size)
              job = Marshal.load(job)
              @logger.info("Getting job #{job}")
              if job == :quit
                exit
              end
              if @proc.call(job)
                @write_io.puts("s")
              else
                @write_io.puts("f")
              end
            end
          rescue => e
            puts job.inspect
            puts e.backtrace
          end
        end
      end
    end
  end
end