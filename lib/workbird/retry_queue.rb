module Workbird
  class RetryQueue
    attr_accessor :jobs, :max_retry

    def initialize(max_retry)
      @jobs = Hash.new
      @job_queue = []
      @max_retry = 10
    end

    def take
      chunk = @job_queue.pop
    end

    def <<(item)
      @jobs[item] ||= 0
      @jobs[item] += 1
      @job_queue.push(item) if @jobs[item] < self.max_retry
    end
  end
end