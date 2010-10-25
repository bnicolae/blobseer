require 'libblobseer_ruby'
#============================================================#
# BlobSeer: Ruby binding
# by Matthieu Dorier 
#    (matthieu.dorier@eleves.bretagne.ens-cacha.fr)
#
# date 08.26.2009 : binding version 0.5
#      works with blobseer version : 0.3.1
#
# date 10.25.2010 : binding version 0.6
#      works with blobseer version : 0.4
#============================================================#

#------------------------------------------------------------#
# PageLocation
# locate segment into the service
#------------------------------------------------------------#
class PageLocation
	
	attr_accessor :host # String, hostname of the provider
	attr_accessor :port # String, port of the provider
	attr_accessor :offset # Integer, offset of segment
	attr_accessor :size # Integer, size of segment

	def initialize(host,port,offset,size)
		@host = host
		@port = port
		@offset = offset
		@size = size
	end
end

#------------------------------------------------------------#
# ObjectHandler
# acts as a gateway to a blob
#------------------------------------------------------------#
class ObjectHandler

	#----------------------------------------------------#
	# initialize the ObjectHandler with a given config
	# file. Raise an error if the config file doesn't 
	# exists.
	def initialize(config)
		if not FileTest.exists?(config)
			raise "#{config} : file not found.\n"
		end
		@cpp_object_handler = BLOBSEER::CppObjectHandler.new(config)
	end

	#----------------------------------------------------#
	# create a new blob with given page size and
	# replica count. Replica count can be ommited,
	# default is 1.
	def create_blob(page_size, replica_count = 1)
		if page_size <= 0
			raise "page size must be > 0."
		end
		return BLOBSEER._create_(@cpp_object_handler, page_size, replica_count)
	end
	
	#----------------------------------------------------#
	# clone a given blob version into a new one that
	# can evolve differently an transparently.
	def clone(id = 0, version = 0)
		return BLOBSEER._clone_(@cpp_object_handler, id, version)
	end
	
	#----------------------------------------------------#
	# get the id of the current blob.
	def get_id
		return BLOBSEER._get_id_(@cpp_object_handler)
	end
	
	#----------------------------------------------------#
	# get the version of the current blob.
	def get_version
		return BLOBSEER._get_version_(@cpp_object_handler)
	end

	#----------------------------------------------------#
	# read into the blob, given offset and size. If
	# version is ommited, default is the latest one.
	def read(offset,size,version=0)
		#if version == -1
		#	version = BLOBSEER._get_version_(@cpp_object_handler)
		#end
		return BLOBSEER._read_(@cpp_object_handler,offset,size,version)
	end
	
	#----------------------------------------------------#
	# write a string into the blob.
	def write(offset,str)
		return BLOBSEER._write_(@cpp_object_handler,offset,str.size,str)
	end

	#----------------------------------------------------#
	# get the latest status of a given blob, using its
	# blob id.
	def get_latest(blob_id)
		return BLOBSEER._get_latest_(@cpp_object_handler, blob_id)
	end
	
	#----------------------------------------------------#
	# get the current page size.
	def get_page_size
		return BLOBSEER._get_page_size_(@cpp_object_handler)
	end

	#----------------------------------------------------#
	# get the current size.
	def get_size(version = 0)
		#if version == -1
		#	version = BLOBSEER._get_version_(@cpp_object_handler)
		#end
		return BLOBSEER._get_size_(@cpp_object_handler, version)
	end

	#----------------------------------------------------#
	# append 'size' bytes of memory, using a string to
	# fill it if necessary.
	def append(size,str = nil)
		if str == nil
			str = "\0"*size
		end
		return BLOBSEER._append_(@cpp_object_handler,size,str)
	end
	
	#----------------------------------------------------#
	# get the names of the providers that are in charge
	# of pages related to a given segment.
	def get_locations(offset, size, version = 0)
		loc = BLOBSEER._get_locations_(@cpp_object_handler,offset,size,version)
		result = []
		loc.each do | a |
			result << PageLocation.new(a[0],a[1],a[2],a[3])
		end
		return result
	end
end
