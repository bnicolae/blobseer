require "blobseer"

PSIZE = 1024
REPC = 1

class Test
	
	def initialize(config)
		@object_handler = ObjectHandler.new(config)

		# creating a blob
		@object_handler.create_blob(PSIZE,REPC)
		
		# trying to append 2 pages of memory
		@object_handler.append(2*PSIZE)
		
		# trying to write something
		buffer = ""
		PSIZE.times do |k|
			buffer << k % 256
		end
		@object_handler.write(0, buffer)
		
		# trying to re-read
		@object_handler.get_latest(@object_handler.get_id);
		result = @object_handler.read(0,1024);
		
		# compare the result with what should be in the blob
		c = true
		PSIZE.times do |k| 
			c = c & (result[k] == (k%256)) 
		end
		p c
		
		# other tests
		p @object_handler.get_size
		p @object_handler.get_page_size
		p @object_handler.get_id
	end
end

if ARGV.size != 1
	print "Usage : ruby test.rb <configfile>\n"
else
	Test.new(ARGV[0])
end

