from __future__ import print_function
import platform
from datetime import datetime

jython = platform.platform().lower().find("java") != -1
import DBAPI
from java.sql import Types
import time

crsr = None
lob_sizes = (
    10000, 20000, 50000,
    100000, 200000, 500000,
    1000000, 2000000, 5000000,
    #    10000000, 20000000,
)
loop_count = 10
times = {}
byte_size = 0
stop_script = False
time_count = 0
byte_intervals = []
batch_size = 2048
size_times = {}
CANCEL_TEST_OUTPUT = "Cleaning up and canceling speed test..."

def test_blob(batch_size):
    if stop_script:
        print(CANCEL_TEST_OUTPUT)
        return

    print("BLOB speed test started on " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " with batch size of " + str(batch_size) + " bytes and " + str(loop_count) + " iterations")
    db = None
    try:
        db = open_dbapi()
        # make/store the blobs
        blob_hash = create_and_store_blob(db)

        if stop_script:
            print(CANCEL_TEST_OUTPUT)
            delete_stored_data(db, lob_sizes)
            return

        retrieve_blob(db, blob_hash, batch_size)

        # report the stats
        print_stats("BLOB")

        # delete blobs
        print("Cleaning up, deleting the BLOB data...")
        time_elapsed_delete = delete_stored_data(db, lob_sizes)
        print("\nBLOBs deleted in %.3f seconds" % time_elapsed_delete)
    finally:
        try:
            if db is not None:
                db.close()
                print("BLOB speed test completed on " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        except Exception as e:
            print("DB cannot be closed", e)

def test_clob(batch_size):
    if stop_script:
        print(CANCEL_TEST_OUTPUT)
        return

    print("CLOB speed test started on " + datetime.now().strftime("%d/%m/%Y %H:%M:%S") + " with batch size of " + str(batch_size) + " bytes and " + str(loop_count) + " iterations")
    db = None
    try:
        db = open_dbapi()

        # make/store the clobs
        clob_hash = create_and_store_clob(db)

        if stop_script:
            print(CANCEL_TEST_OUTPUT)
            delete_stored_data(db, lob_sizes)
            return

        retrieve_clob(db, clob_hash, batch_size)

        # report the averages
        print_stats("CLOB")

        # delete and clean up data created for the test
        print("Cleaning up, deleting the CLOB data...")
        time_elapsed_delete = delete_stored_data(db, lob_sizes)
        print("\nCLOBs deleted in %.3f seconds" % time_elapsed_delete)

    except SystemExit:
        pass
    finally:
        try:
            if db is not None:
                db.close()
                print("CLOB speed test completed on " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        except Exception as e:
            print("DB cannot be closed", e)

def open_dbapi():
    try:
        db = DBAPI.open()
        return db
    except Exception as e:
        print("error opening DBAPI...", e)

def open_conn(db):
    try:
        conn = db.getConnection()
        return conn
    except:
        print("error opening connection...")

def create_and_store_blob(db):
    make_blob_sql = '''
    DECLARE 
        l_len   INTEGER := :1; 
        l_block VARCHAR2(10000); 
        l_blob  BLOB; 
    BEGIN 
        l_block := dbms_random.String('p', 4000); 
        l_blob := utl_raw.Cast_to_raw(l_block); 
    
        WHILE dbms_lob.Getlength(l_blob) < l_len LOOP 
            l_block := dbms_random.String('p', 2000); 
            dbms_lob.OPEN(l_blob, dbms_lob.lob_readwrite); 
            dbms_lob.Writeappend(l_blob, 
            utl_raw.Length(utl_raw.Cast_to_raw(l_block)), 
            utl_raw.Cast_to_raw(l_block)); 
            dbms_lob.CLOSE(lob_loc => l_blob); 
        END LOOP; 
    
        :2 := l_blob; 
    END; 

    '''
    store_blob_sql = '''
    DECLARE 
        l_binary_code       NUMBER; 
        l_binary            BLOB; 
        l_id                VARCHAR2(162); 
        l_media_type_or_ext VARCHAR2(80); 
        l_description       VARCHAR2(162); 
        l_fail_if_exists    VARCHAR2(50); 
        l_ignore_nulls      VARCHAR2(50); 
        l_office_id         VARCHAR2(50); 
    BEGIN 
        l_media_type_or_ext := 'other/unknown'; 
        l_description := current_timestamp; --'database speed test data'; 
        l_fail_if_exists := 'F'; 
        l_ignore_nulls := 'T'; 
        l_id := '/TEST/DATA_BLOCK/SIZE='      || :1; 
        l_binary := :2; 

        cwms_text.Store_binary(l_binary_code, l_binary, l_id, l_media_type_or_ext, 
        l_description, l_fail_if_exists, l_ignore_nulls, l_office_id); 
    END; 
    '''

    conn = None
    make_blob_stmt = None
    store_blob_stmt = None
    blob_hash = {}

    try:
        conn = open_conn(db)
        if conn is None:
            return blob_hash
        print("Creating and storing BLOB data...")
        ts1 = time.time()

        for i in lob_sizes:
            if stop_script:
                print(CANCEL_TEST_OUTPUT)
                # delete what has been stored
                sizes_index = lob_sizes.index(i) - 1
                delete_stored_data(db, lob_sizes[:sizes_index])
                return
            # making the blob
            make_blob_stmt = conn.prepareCall(make_blob_sql)
            make_blob_stmt.setInt(1, i)
            make_blob_stmt.registerOutParameter(2, Types.BLOB)
            make_blob_stmt.execute()
            blob = make_blob_stmt.getBlob(2)

            # getting blob into string to hash, to compare store and retrieve values later
            blob_stream = blob.getBinaryStream()
            bytes_array = bytearray(blob.length())
            blob_stream.read(bytes_array)
            # store the hash of the bytes in the bytes array
            blob_hash[i] = hash(bytes(bytes_array))

            # storing the blob
            store_blob_stmt = conn.prepareCall(store_blob_sql)
            store_blob_stmt.setInt(1, i)
            store_blob_stmt.setBlob(2, blob)
            store_blob_stmt.execute()
            print("%s-byte BLOB created and stored" % '{:,}'.format(i))

        conn.commit()
    finally:
        try:
            if make_blob_stmt is not None:
                make_blob_stmt.close()
        except Exception as e:
            print("Callable statement for making BLOB cannot be closed", e)
        try:
            if store_blob_stmt is not None:
                store_blob_stmt.close()
        except Exception as e:
            print("Callable statement for storing BLOB cannot be closed", e)
        try:
            if conn is not None:
                conn.close()
        except Exception as e:
            print("Connection after creating and storing BLOB cannot be closed", e)

    ts2 = time.time()
    print("All BLOBs created and stored in %.3f seconds" % (ts2 - ts1))
    return blob_hash

def create_and_store_clob(db):
    make_clob_sql = '''
    DECLARE 
        l_len   INTEGER := :1; 
        l_block VARCHAR2(10000); 
        l_clob  CLOB; 
    BEGIN 
        l_block := dbms_random.String('p', 4000); 
        l_clob := To_clob(l_block); 
    
        WHILE dbms_lob.Getlength(l_clob) < l_len LOOP 
            l_block := dbms_random.String('p', 2000); 
            dbms_lob.OPEN(l_clob, dbms_lob.lob_readwrite); 
            dbms_lob.Writeappend(l_clob, 
            utl_raw.Length(utl_raw.Cast_to_raw(l_block)), 
            utl_raw.Cast_to_raw(l_block)); 
            dbms_lob.CLOSE(lob_loc => l_clob); 
        END LOOP; 
    
        :2 := l_clob; 
    END; 
    '''
    store_clob_sql = '''
        DECLARE 
            l_text_code      NUMBER; 
            l_text           CLOB; 
            l_id             VARCHAR2(162); 
            l_description    VARCHAR2(162); 
            l_fail_if_exists VARCHAR2(50); 
            l_office_id      VARCHAR2(50); 
        BEGIN 
            l_description := current_timestamp; 
            l_fail_if_exists := 'F'; 
            l_id := '/TEST/DATA_BLOCK/SIZE='      || :1; 
            l_text := :2; 
        
            cwms_text.Store_text(l_text_code, l_text, l_id, l_description, 
            l_fail_if_exists, l_office_id); 
        END; 
        '''
    conn = None
    make_clob_stmt = None
    store_clob_stmt = None
    clob_hash = {}
    try:
        conn = open_conn(db)
        if conn is None:
            return clob_hash
        print("Creating and storing CLOB data...")
        # time includes making, storing, and hashing the clob
        ts1 = time.time()

        for i in lob_sizes:
            if stop_script:
                print(CANCEL_TEST_OUTPUT)
                # delete what has been stored
                sizes_index = lob_sizes.index(i) - 1
                delete_stored_data(db, lob_sizes[:sizes_index])
                return
            # making the clob
            make_clob_stmt = conn.prepareCall(make_clob_sql)
            make_clob_stmt.setInt(1, i)
            make_clob_stmt.registerOutParameter(2, Types.CLOB)
            make_clob_stmt.execute()
            clob = make_clob_stmt.getClob(2)
            clob_length = clob.length()

            # hash stream to make sure stored and retrieved blob are identical
            char_stream = clob.getCharacterStream(1, clob_length)
            chars = '\0' * clob_length
            char_array = char_stream.read(chars)
            clob_hash[i] = hash(char_array)

            # storing the clob
            store_clob_stmt = conn.prepareCall(store_clob_sql)
            store_clob_stmt.setInt(1, i)
            store_clob_stmt.setClob(2, clob)
            store_clob_stmt.execute()

            print("%s-byte CLOB created and stored" % '{:,}'.format(i))

            conn.commit()
    finally:
        try:
            if make_clob_stmt is not None:
                make_clob_stmt.close()
        except Exception as e:
            print("CallableStatement for creating data cannot be closed", e)
        try:
            if store_clob_stmt is not None:
                store_clob_stmt.close()
        except Exception as e:
            print("CallableStatement for storing data cannot be closed", e)
        try:
            if conn is not None:
                conn.close()
        except Exception as e:
            print("Connection after creating and storing CLOB cannot be closed", e)

    ts2 = time.time()
    print("All CLOBs created and stored in %.3f seconds" % (ts2 - ts1))
    return clob_hash

def retrieve_blob(db, blob_hash, batch_size):
    global size_times
    conn = None
    stmt = None
    try:
        conn = open_conn(db)
        if conn is None:
            return
        print("Now retrieving the BLOBs...")
        retrieve_time_start = time.time()
        sql = "select cwms_text.retrieve_binary(:id, CWMS_SEC.GET_USER_OFFICE_ID()) from dual"
        if stop_script:
            print(CANCEL_TEST_OUTPUT)
            delete_stored_data(db, lob_sizes)
            return

        stmt = conn.prepareStatement(sql)

        for size in lob_sizes:
            for _ in range(loop_count):
                time_start = time.clock()
                if stop_script:
                    print(CANCEL_TEST_OUTPUT)
                    delete_stored_data(db, lob_sizes)
                    return

                blob_id = "/TEST/DATA_BLOCK/SIZE=%d" % size
                # retrieving the blob
                stmt.setString(1, blob_id)
                try:
                    rs = stmt.executeQuery()
                    rs.next()
                    blob = rs.getBlob(1)
                    blob_length = blob.length()

                    blob_stream = blob.getBinaryStream()
                    bytes_array = bytearray(blob.length())
                    blob_stream.read(bytes_array)
                    if hash(bytes(bytes_array)) != blob_hash[blob_length]:
                        hash_error_msg = "BLOB " + str(blob_length) + " stored hash and retrieved hash are not the same"
                        raise TypeError(hash_error_msg)

                    # reset character stream for batch processing
                    blob_stream = blob.getBinaryStream()

                    # the whole batch is the whole blob
                    if batch_size > blob_length:
                        bytes_array = bytearray(blob_length)
                        read_blob(bytes_array, blob_stream)
                    else:
                        # getting each batch
                        bytes_array = bytearray(batch_size)
                        for _ in range(1, blob_length, batch_size):
                            read_blob(bytes_array, blob_stream)

                    blob.free()
                    # verify retrieval with hash
                    time_end = time.clock()

                    if size in size_times:
                        size_times[size].append(float(time_end - time_start))
                    else:
                        size_times[size] = [float(time_end - time_start)]
                finally:
                    rs.close()
            conn.commit()
        print("All BLOBs retrieved in %.3f seconds " % (time.time() - retrieve_time_start))
    finally:
        try:
            if stmt is not None:
                stmt.close()
        except Exception as e:
            print("CallableStatement for retrieving data cannot be closed", e)
        try:
            if conn is not None:
                conn.close()
        except Exception as e:
            print("Connection after retrieving data cannot be closed", e)

def read_blob(bytes, blob_stream):
    batch_start = time.clock()
    bytes_read = blob_stream.read(bytes)
    batch_end = time.clock()
    batch_time_elapsed = batch_end - batch_start
    add_bytes("BLOB", bytes_read, batch_time_elapsed)

def retrieve_clob(db, clob_hash, batch_size):
    conn = None
    stmt = None
    try:
        conn = open_conn(db)
        if conn is None:
            return
        # string formatting
        print("Now retrieving CLOB data...")
        retrieve_time_start = time.time()
        retrieve_clob_sql = "select cwms_text.retrieve_text(:id, CWMS_SEC.GET_USER_OFFICE_ID()) from dual"

        stmt = conn.prepareStatement(retrieve_clob_sql)
        for size in lob_sizes:
            for _ in range(loop_count):
                time_start = time.clock()
                if stop_script:
                    print(CANCEL_TEST_OUTPUT)
                    delete_stored_data(db, lob_sizes)
                    return
                clob_id = "/TEST/DATA_BLOCK/SIZE=%d" % size
                try:
                    # retrieve clob
                    stmt.setString(1, clob_id)
                    rs = stmt.executeQuery()
                    rs.next()
                    clob = rs.getClob(1)
                    clob_length = clob.length()

                    # verify retrieval with hash
                    clob_stream = clob.getCharacterStream()
                    chars = '\0' * clob_length
                    chars_read = clob_stream.read(chars)
                    if hash(chars_read) != clob_hash[clob_length]:
                        hash_error_msg = str(clob_length) + " stored hash and retrieved hash are not the same"
                        raise TypeError(hash_error_msg)

                    # reset character stream for batch processing
                    clob_stream = clob.getCharacterStream()

                    # 2 bytes in 1 character, so if we want 2048 byte batch, we retrieve 1024 characters
                    batch_size_in_chars = batch_size / 2

                    # if the batch is the entire clob
                    if batch_size_in_chars > clob_length:
                        read_clob(clob_length, clob_stream)
                    else:
                        # batch process
                        for _ in range(1, clob_length, batch_size_in_chars):
                            read_clob(batch_size_in_chars, clob_stream)
                    clob.free()

                    time_end = time.clock()
                    if size in size_times:
                        size_times[size].append(float(time_end - time_start))
                    else:
                        size_times[size] = [float(time_end - time_start)]
                finally:
                    rs.close()
            conn.commit()
        print("All CLOBs retrieved in %.3f seconds " % (time.time() - retrieve_time_start))
    finally:
        try:
            if stmt is not None:
                stmt.close()
        except Exception as e:
            print("CallableStatment for retrieving data cannot be closed", e)
        try:
            if conn is not None:
                conn.close()
        except Exception as e:
            print("Connection after retrieving data cannot be closed", e)

def read_clob(size_to_read, clob_stream):
    chars = "\0" * size_to_read
    batch_start = time.time()
    chars_read = clob_stream.read(chars)
    batch_end = time.time()
    batch_time_elapsed = batch_end - batch_start
    bytes_read = chars_read * 2
    add_bytes("CLOB", bytes_read, batch_time_elapsed)

def delete_stored_data(db, sizes_to_delete):
    conn = None
    stmt = None
    try:
        conn = open_conn(db)
        if conn is None:
            return
        sql = "begin\n" + "\n".join(["cwms_text.delete_binary('/TEST/DATA_BLOCK/SIZE=%d');" % size for size in sizes_to_delete]) + "\nend;"
        ts1 = time.time()
        stmt = conn.prepareStatement(sql)
        stmt.execute()

        conn.commit()
        ts2 = time.time()

    finally:
        try:
            if stmt is not None:
                stmt.close()
        except Exception as e:
            print("CallableStatment for deleting data cannot be closed", e)
        try:
            if conn is not None:
                conn.close()
        except Exception as e:
            print("Connection after deleting data cannot be closed", e)

    return ts2 - ts1

def run_blob_and_clob():
    test_blob()
    test_clob()
    print("Database Speed Tester completed on " + datetime.now().strftime("%d/%m/%Y %H:%M:%S"))

def stop():
    global stop_script
    stop_script = True

def set_batch_size(size):
    global batch_size
    print("Setting batch_size to %s" % '{:,}'.format(size))
    batch_size = size

def add_bytes(type, batch_size, time_elapsed):
    global time_count, byte_size, byte_intervals
    time_count = time_count + time_elapsed
    byte_size = byte_size + batch_size

    if time_count >= .1:
        mbps = (byte_size / time_count) / 1000000

        output = str(type) + ": %.3f mb/s (%s bytes, %.3f seconds)" % (mbps, '{:,}'.format(byte_size), time_count)
        print(output)
        # clean up and restart count for next batch

        byte_intervals.append(mbps)
        time_count = 0
        byte_size = 0

def print_stats(type):
    global byte_intervals, size_times
    for s in lob_sizes:
        min_time = min(size_times[s])
        max_time = max(size_times[s])
        average_time = float(sum(size_times[s])) / float(len(size_times[s]))
        #  format_size
        print(
            "%s: %d x %s-byte %ss retrieved in min: %0.3f mb/s max: %0.3f mb/s average: %.3f mb/s" % (type, loop_count, '{:,}'.format(s), type, float(min_time), float(max_time), float(average_time)))
    print(str(type) + " total: min: %0.3f mb/s max: %0.3f mb/s average: %.3f mb/s " % (min(byte_intervals), max(byte_intervals), sum(byte_intervals) / len(byte_intervals)))
    byte_intervals = []
    size_times = {}
