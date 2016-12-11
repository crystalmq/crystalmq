require "socket"
require "./lib.cr"

s = CMQ::Producer.new("localhost", ARGV[0])
s.write(ARGV[1])
s.terminate