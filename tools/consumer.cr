require "socket"
require "./lib.cr"

messages = 0

start = Time.now

CMQ::Consumer.new("localhost", ARGV[0], ARGV[1]).consume do |message|
  start = Time.now if messages == 0
  messages = messages + 1
  puts messages
  #break if messages == 100000
end

puts "Total Time: #{Time.now - start}"
