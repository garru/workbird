module Workbird
  class Range
    attr_accessor :start_id, :end_id, :start_time, :current_id

    def initialize(start_id, end_id)
      self.start_id = start_id
      self.end_id = end_id
      self.current_id = self.start_id
      self.start_time
    end

    def take(chunk_size)
      self.start_time = Time.now unless self.start_time
      return false if self.current_id >= self.end_id
      chunks = [chunk_size, (self.end_id - self.current_id)].min
      give = (self.current_id .. (self.current_id + chunks - 1)).to_a
      self.current_id += chunks
      give
    end

    def avg_time(current_time = Time.now)
      (current_time.to_f - self.start_time.to_f) / (self.current_id - self.start_id)
    end

    def progress
      self.current_id - self.start_id
    end

    def eta
      return 0 if self.current_id == self.start_id
      (self.end_id - self.current_id) * avg_time
    end
  end
end