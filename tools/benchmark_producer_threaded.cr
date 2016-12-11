require "socket"
require "./lib.cr"

channel = ARGV[0]
message = ARGV[1]

number_of_threads = 200
number_of_runs = 1000

channels = Array(Channel(Nil)).new

sent_messages = 0
number_of_threads.times do
  spawn do
    producer = CMQ::Producer.new("localhost", ARGV[0])
    r_channel = Channel(Nil).new
    channels << r_channel
    sleep 2
    number_of_runs.times do
      start = Time.now
      producer.write(sent_messages.to_s)
      sent_messages = sent_messages + 1
    end
    
    r_channel.send(nil)
    producer.terminate
  end
end

sleep 2
start = Time.now
puts "Benchmark Started..."
channels.each { |c| c.receive }
puts "#{sent_messages} Messages done in #{Time.now - start}"
