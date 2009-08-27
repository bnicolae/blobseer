require 'libblobseer_ruby'
#============================================================#
# BlobSeer: Ruby binding
# by Matthieu Dorier (matthieu.dorier@irisa.fr)
#      blobseer version : 0.3.1
#      ruby binding version: 0.5       date: 08.26.2009
#============================================================#
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
	def read(offset,size,version=-1)
		if version == -1
			version = BLOBSEER._get_version_(@cpp_object_handler)
		end
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
	def get_size(version = -1)
		if version == -1
			version = BLOBSEER._get_version_(@cpp_object_handler)
		end
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
end
