require 'libblobseer_ruby'
#============================================================#
# BlobSeer: Ruby binding
# by Matthieu Dorier (matthieu.dorier@irisa.fr)
#      blobseer version : 0.3.1
#      ruby binding version: 0.4       date: 6.17.2009
#============================================================#
class ObjectHandler

	#----------------------------------------------------#
	# initialize the ObjectHandler with a given config
	# file. Raise an error if the config file doesn't 
	# exists.
	def initialize(config)
		@blob_entry = BLOBSEER::Blob_t.new
		if not FileTest.exists?(config)
			raise "#{config} : file not found.\n"
		end
		@environment = BLOBSEER._init_(config)
		@initialize = false
	end

	#----------------------------------------------------#
	# create a new blob with given page size and
	# replica count. Replica count can be ommited,
	# default is 1.
	def create_blob(page_size, replica_count = 1)
		if page_size <= 0
			raise "page size must be > 0."
		end
		@blob_entry =  BLOBSEER._create_(@environment, page_size, replica_count)
		if @blob_entry == nil
			raise "while creating a blob."
		end
		@initialized = true
	end
	
	#----------------------------------------------------#
	# get the id of the current blob.
	def get_id
		if @initialized
			return BLOBSEER._getid_(@blob_entry)
		else
			return 0
		end
	end
	
	#----------------------------------------------------#
	# get the version of the current blob.
	def get_version
		if @initialized
			return BLOBSEER._getversion_(@blob_entry)
		else
			return 0
		end
	end

	#----------------------------------------------------#
	# read into the blob, given offset and size. If
	# version is ommited, default is the latest one.
	def read(offset,size,version=-1)
		if @initialized
			if version == -1
				version = BLOBSEER._getversion_(@blob_entry)
			end
			return BLOBSEER._read_(@blob_entry,version,offset,size)
		else
			raise "blob must be initialized."
		end
	end
	
	#----------------------------------------------------#
	# write a string into the blob.
	def write(offset,str)
		if @initialized
			if BLOBSEER._write_(@blob_entry,offset,str.size,str)
				return true
			else
				return false
			end
		else
			raise "blob must be initialized."
		end
	end

	#----------------------------------------------------#
	# get the latest status of a given blob, using its
	# blob id.
	def get_latest(blob_id)
		@blob_entry = BLOBSEER._setid_(@environment, blob_id,@blob_entry)
		BLOBSEER._getlatest_(@blob_entry)
		if @blob_entry == nil
			raise "while connecting to the blob."
		end
		@initialized = true
		return blob_id
	end
	
	#----------------------------------------------------#
	# get the current page size.
	def get_page_size
		if @initialized
			return BLOBSEER._getpagesize_(@blob_entry)
		else
			return 0
		end
	end

	#----------------------------------------------------#
	# get the current size.
	def get_size(version = -1)
		if @initialized
			if version != -1
				return BLOBSEER._getsize_(@blob_entry, version)
			else
				return BLOBSEER._getcurrentsize_(@blob_entry)
			end
		else
			raise "blob must be initialized."
		end
	end

	#----------------------------------------------------#
	# append 'size' bytes of memory, using a string to
	# fill it if necessary.
	def append(size,str = nil)
		if @initialized
			if str == nil
				str = "\0"*size
			end
			if BLOBSEER._append_(@blob_entry,size,str)
				return true
			else
				return false
			end
		else
			raise "blob must be initialized."
		end
	end
end
