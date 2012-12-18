! To compile this program, use
! 	gfortran -o testf test.f90 libblobseer-f.so
! Make sure libblobseer-f.so is in your LD_LIBRARY_PATH
! before running the program.

PROGRAM test
    IMPLICIT NONE

    INTEGER*8 :: environment
    INTEGER*8 :: blob
    INTEGER*4 :: err
    INTEGER*8, PARAMETER :: psize = 8
    INTEGER*4, PARAMETER :: replica = 1
    CHARACTER(LEN=8), PARAMETER :: config = 'test.cfg'
    CHARACTER, DIMENSION(64) :: buffer
    INTEGER*8 :: blob_size
    INTEGER*4 :: blob_id
    INTEGER*8 :: woffset, wsize
    INTEGER*4 :: i

    DO i = 1,64
        buffer(i) = 'A'
    ENDDO
   
    PRINT *, 'Testing BlobSeer using Fortran...'
    call blob_init(environment,config)

    call blob_create(environment,psize,replica,blob,err)
    IF(err.eq.1) THEN
        PRINT *, 'Blob created successfuly'
    ELSE
        call EXIT(1)
    ENDIF

    call blob_get_size(blob,1,blob_size)
    PRINT *, 'Initial Blob size is ', blob_size

    call blob_get_id(blob,blob_id)
    PRINT *, 'Blob ID is ', blob_id

    woffset = 0
    wsize = 8
    call blob_write(blob,woffset,wsize,'BBBBBBBB',err)

    woffset = 0
    wsize = 16
    call blob_write(blob,woffset,wsize,'FFFFFFFFCCCCCCCC',err)

    woffset = 16
    wsize = 8
    call blob_write(blob,woffset,wsize,'DDDDDDDD',err)
    
    woffset = 16
    wsize = 16
    call blob_write(blob,woffset,wsize,'EEEEEEEEEEEEEEEE',err)

    call blob_get_size(blob,4,blob_size)
    PRINT *, 'After the writes, size is ', blob_size

    woffset = 0
    wsize = 32
    call blob_read(blob,4,woffset,wsize,buffer,err)
    PRINT *, 'Read: ', buffer

    call blob_free(environment,blob)
    call blob_finalize(environment)
END PROGRAM
