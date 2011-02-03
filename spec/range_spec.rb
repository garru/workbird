require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe "Workbird::Range" do
  before do
    @start_id = 0
    @end_id = 100
    @chunk_size = 10
    @r = Workbird::Range.new(@start_id, @end_id)
  end
  describe "#initialize" do
    it "should set up start_id and end_id" do
      @r.start_id.should == @start_id
      @r.end_id.should ==  @end_id
    end
  end

  describe "#take(chunk_size)" do
    it "should return ids of chunk_size" do
      result = @r.take(@chunk_size)
      result.should == (@start_id ... @start_id + @chunk_size).to_a
      result = @r.take(@chunk_size)
      next_start_id = @start_id + @chunk_size
      result.should == (next_start_id ... next_start_id + @chunk_size).to_a
    end

    it "should set start time on first take" do
      @r.start_time.should == nil
      @r.take(1)
      @r.start_time.should_not == nil
    end
  end

  describe "#avg_time" do
    it "should calculate avg time between takes per id" do
      @r.start_time = 0
      @r.current_id = 100
      @r.avg_time(100).should == 1
    end
  end

  describe "#eta" do
    it "should return nil if there isn't enough information to calculate eta" do
      @r.eta.should == nil
    end
    it "should calculate the eta of completion based off of history of actions" do
      mock(Time).now{100}
      @r.start_time = 0
      @r.current_id = 50
      @r.eta.should == 100.to_f
    end
  end
end
