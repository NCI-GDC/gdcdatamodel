import os, sys
#import yaml
import boto.s3.connection
from boto.s3.key import Key
from cStringIO import StringIO as BIO
import md5
import time

class S3_Wrapper:

    SIZE_GB = 1000000000
    SIZE_TB = 1000000000000


    def __init__(self):
        # default info for each, we could look at env
        # also, I've seen secure=False used with boto,
        # so I mirrored that use
        self.s3_inst_info = { 
            'ceph': {
                'secure': False,
                'url': 'ceph.service.consul'
            }, 
            'cleversafe': {
                'secure': False,
                'url': 'cleversafe.service.consul'
            } 
        }

        # being a bit smart about getting the env data, in case we switch
        # VMs
        for env in os.environ.keys():
            if "ACCESS_KEY" in env:
                if "CEPH" in env:
                    self.s3_inst_info['ceph']['access_key'] = os.environ[env]
                elif "CLEVERSAFE" in env:
                    self.s3_inst_info['cleversafe']['access_key'] = os.environ[env]
                else:
                    self.s3_inst_info['ceph']['access_key'] = os.environ[env]
                    self.s3_inst_info['cleversafe']['access_key'] = os.environ[env]
            if "SECRET_KEY" in env:
                if "CEPH" in env:
                    self.s3_inst_info['ceph']['secret_key'] = os.environ[env]
                elif "CLEVERSAFE" in env:
                    self.s3_inst_info['cleversafe']['secret_key'] = os.environ[env]
                else:
                    self.s3_inst_info['ceph']['secret_key'] = os.environ[env]
                    self.s3_inst_info['cleversafe']['secret_key'] = os.environ[env]


    def get_nearest_file_size(self, size):
        sizes = [
            (1000000000000000, "PB"),
            (1000000000000, "TB"),
            (1000000000, "GB"),
            (1000000, "MB"),
            (1000, "KB") 
        ]
 
        value = sizes[len(sizes) - 1]
        for entry in sizes:
            if size < entry[0]:
                continue
            value = entry
            break
 
        return value

    # does a simple s3 check to see if we can connect
    def check_s3(self, which_s3, url_offset = 0):
        s3_ok = False
        if type(self.s3_inst_info[which_s3]['url']) == list:
            if url_offset >= len(self.s3_inst_info[which_s3]['url']):
                url_offset = 0
            host_url = self.s3_inst_info[which_s3]['url'][url_offset]
        else:
            host_url = self.s3_inst_info[which_s3]['url']
        if which_s3 in self.s3_inst_info:
            logger.info('Checking that s3 is reachable')
            if self.s3_inst_info[which_s3]['secure']:
                r = requests.get('https://{}'.format(host_url))
            else:
                r = requests.get('http://{}'.format(host_url))

            if r.status_code != 200:
                logging.error('Status: {}'.format(r.status_code))
                raise Exception('s3 unreachable at {}'.format(host_url))
            else:
                s3_ok = True
                logger.info('Found s3 gateway at {}'.format(host_url))

        return s3_ok

    # connects to an s3 instance, using the environment vars as set
    def connect_to_s3(self, which_s3, url_offset = 0):
        if which_s3 in self.s3_inst_info:
            if type(self.s3_inst_info[which_s3]['url']) == list:
                host_url = self.s3_inst_info[which_s3]['url'][url_offset]
            else:
                host_url = self.s3_inst_info[which_s3]['url']
            conn = boto.connect_s3(
                aws_access_key_id=self.s3_inst_info[which_s3]['access_key'],
                aws_secret_access_key=self.s3_inst_info[which_s3]['secret_key'],
                host=self.s3_inst_info[which_s3]['url'],
                is_secure=self.s3_inst_info[which_s3]['secure'],
                calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                )
        return conn

    # upload a file to an s3 instance
    def upload_file(self, conn, bucket_name, file_name, calc_md5=False):
        self.md5_sum = md5.new()
        bucket_exists = False
        bucket_list = conn.get_all_buckets()
        for instance in bucket_list:
            if instance.name == bucket_name:
                bucket_exists = True
                break

        if bucket_exists:
            bucket = conn.get_bucket(bucket_name)
        else:
            bucket = conn.create_bucket(bucket_name)

        new_key = Key(bucket)
        if file_name.find("/") != -1:
            key_name = file_name.split("/")[-1]
        else:
            key_name = file_name

        new_key.key = key_name
        new_key.set_contents_from_filename(file_name)

    # upload a file to an s3 instance as a multipart upload
    # it uses a stream size of 1 GiB per chunk to write
    def upload_multipart_file(self, conn, bucket_name, key_name, file_iterator, calc_md5=False, save_local=False):
        self.md5_sum = md5.new()
        bucket_exists = False
        if 'https_proxy' in os.environ:
            del os.environ['https_proxy']
        if 'http_proxy' in os.environ:
            del os.environ['http_proxy']
        bucket_exists = False
        try:
            bucket_list = conn.get_all_buckets()
        except:
            raise RuntimeError
        else:
            for instance in bucket_list:
                if instance.name == bucket_name:
                    bucket_exists = True
                    break

        if bucket_exists:
            bucket = conn.get_bucket(bucket_name)
        else:
            bucket = conn.create_bucket(bucket_name)

        stream_buffer = BIO()
        mp_chunk_size = 1073741824 # 1GiB
        cur_size = 0
        chunk_index = 1
        total_size = 0
        mp = bucket.initiate_multipart_upload(key_name)
        start_time = time.clock()

        # loop over the file, writing a chunk at a time
        for chunk in file_iterator:
            size_info = self.get_nearest_file_size(total_size)
            cur_time = time.clock()
            base_transfer_rate = float(total_size) / float(cur_time - start_time)
            transfer_info = self.get_nearest_file_size(base_transfer_rate)
            cur_conv_size = float(total_size) / float(size_info[0])
            cur_conv_rate = base_transfer_rate / float(transfer_info[0])
            sys.stdout.write("%7.02f %s : %6.02f %s per sec\r" % (
                cur_conv_size, size_info[1],
                cur_conv_rate, transfer_info[1]))
            sys.stdout.flush()
            stream_buffer.write(chunk)
            cur_size += len(chunk)
            total_size += len(chunk)

            if calc_md5:
                self.md5_sum.update(chunk)

            if cur_size >= mp_chunk_size:
                os.environ['http_proxy'] = "http://cloud-proxy:3128"
                os.environ['https_proxy'] = "http://cloud-proxy:3128"
                stream_buffer.seek(0)
                if save_local:
                    with open("tmp_" + key_name.split('/')[1], "wb") as save_file:
                        for chunk in stream_buffer:
                            save_file.write(chunk)
                    stream_buffer.seek(0)
                try:
                    result = mp.upload_part_from_file(stream_buffer, chunk_index)
                except:
                    raise RuntimeError
                else:
                    cur_size = 0
                    stream_buffer = BIO()
                    chunk_index += 1
                del os.environ['https_proxy']
                del os.environ['http_proxy']


        # write the remaining data
        os.environ['http_proxy'] = "http://cloud-proxy:3128"
        os.environ['https_proxy'] = "http://cloud-proxy:3128"
        stream_buffer.seek(0)
        try:
            result = mp.upload_part_from_file(stream_buffer, chunk_index)
        except:
            raise RuntimeError
        else:
            cur_size = 0
        del os.environ['https_proxy']
        del os.environ['http_proxy']

        cur_time = time.clock()
        size_info = self.get_nearest_file_size(total_size)
        base_transfer_rate = float(total_size) / float(cur_time - start_time)
        transfer_info = self.get_nearest_file_size(base_transfer_rate)
        cur_conv_size = float(total_size) / float(size_info[0])
        cur_conv_rate = base_transfer_rate / float(transfer_info[0])

        if cur_size > 0:
            os.environ['http_proxy'] = "http://cloud-proxy:3128"
            os.environ['https_proxy'] = "http://cloud-proxy:3128"
            stream_buffer.seek(0)
            try:
                result = mp.upload_part_from_file(stream_buffer, chunk_index)
            except:
                raise RuntimeError

        mp.complete_upload()
        return {"md5_sum" : str(self.md5_sum.hexdigest()), "bytes_transferred" : total_size }


    # get a count of all keys in a specified s3 bucket
    def get_count_of_keys_in_s3_bucket(self, conn, bucket_name):
        bucket = conn.get_bucket(bucket_name)
        rs = bucket.list()
        file_count = 0
        for key in rs:
            file_count = file_count + 1

        return file_count

    # get a file key in a bucket
    def get_file_key(self, conn, bucket_name, file_name):
        key = None
        try:
            bucket = conn.get_bucket(bucket_name)
        except:
            type1, value1, traceback1 = sys.exc_info()
        else:
            key = bucket.get_key(file_name)
        return key
   
    # delete a key in a bucket
    def delete_file_key(self, conn, bucket_name, file_name):
        key = self.get_file_key(conn, bucket_name, file_name)
        key.delete()

    # compare the size of an s3 key by the two methods boto
    # can use to get key size: iterating over keys in a bucket,
    # or getting a key directly by key name
    # NB: we found differences in these on ceph, so this routine
    # allows for checking to see if there's a discrepancy in the
    # file sizes
    def compare_s3_sizes_by_methods(self, conn, bucket_name):
        file_list = []
        bucket = conn.get_bucket(bucket_name)
        rs = bucket.list()
        zero_size_files = 0
        mismatched_files = 0
        num_files = self.get_count_of_keys_in_s3_bucket(conn, bucket_name)
        file_count = 0
        digits = len(str(num_files))
        for key in rs:
            file_count = file_count + 1
            data = bucket.get_key(key.name)
            file_info = {}
            file_info['file_name'] = key.name
            file_info['list_size'] = key.size
            file_info['key_size'] = data.size
            file_list.append(file_info)
            format_str = "%%0%dd/%%0%dd\r" % (digits, digits)
            sys.stdout.write(format_str % (file_count, num_files))
            sys.stdout.flush()
            if (key.size == 0) or (data.size == 0):
                zero_size_files = zero_size_files + 1
            if key.size != data.size:
                mismatched_files = mismatched_files + 1

        return file_list

    # get all the files in a given s3 bucket into a list
    def get_files_in_s3_bucket(self, conn, bucket_name, check_download=False):
        file_list = []
        bucket = conn.get_bucket(bucket_name)
        rs = bucket.list()
        unreachable_files = 0
        zero_size_files = 0
        animation = [ '/', '|', '\\', '-' ]
        animation_index = 0
        num_files = self.get_count_of_keys_in_s3_bucket(conn, bucket_name)
        file_count = 0
        digits = len(str(num_files))
        for key in rs:
            file_count = file_count + 1
            file_list.append((key.name, key.size))
            format_str = "%%0%dd/%%0%dd\r" % (digits, digits)
            sys.stdout.write(format_str % (file_count, num_files))
            sys.stdout.flush()
            animation_index = animation_index + 1
            if key.size == 0:
                zero_size_files = zero_size_files + 1
            else:
                if check_download:
                    try:
                        data = key.read(size=1024)
                    except:
                        unreachable_files = unreachable_files + 1
        return file_list

    # get all the files in all the buckets on a given
    # s3 connection (should be renamed, does more than get counts)
    def get_s3_file_counts(self, conn, which_s3):
        if self.check_s3(which_s3):
            rs = conn.get_all_buckets()
            self.all_files = {}
            for entry in rs:
                if entry.name in self.allowed_buckets:
                    self.all_files[entry.name] = self.get_files_in_s3_bucket(conn, entry.name)
            self.check_time = datetime.datetime.now()

    # after calling get_s3_file_counts, this routine will dump all the
    # keys into a file
    def save_s3_file_lists(self, which_s3):
        for key, value in self.all_files.iteritems():
            bucket_filename = "%s_%s_files_%04d-%02d-%02d_%02d-%02d-%02d.json" % (which_s3, 
                key, datetime.datetime.now().year, datetime.datetime.now().month, 
                datetime.datetime.now().day, datetime.datetime.now().hour, 
                datetime.datetime.now().minute, datetime.datetime.now().second
            )
            with open(bucket_filename, "w") as json_file:
                psql_test = Psqlgraph_Test()
                with psql_test.driver.session_scope() as session:
                    for entry in value:
                        file_entry = {}
                        file_entry['file_name'] = entry[0]
                        file_entry['file_size'] = int(entry[1])
                        if len(file_entry['file_name'].split("/")) > 1:
                            file_key = file_entry['file_name'].split("/")[0]
                            file_name = file_entry['file_name'].split("/")[1]
                            file_info = self.psql_search_for_filename(psql_test, file_name, file_key)
                            if file_info != None:
                                if "md5sum" in file_info:
                                    file_entry['md5sum'] = file_info['md5sum']
                                if "file_size" in file_info:
                                    file_entry['db_size'] = file_info["file_size"]

                            if 'db_size' in file_entry:
                                if file_entry['db_size'] != file_entry['file_size']:
                                    file_entry['size_compare'] = "mismatch"
                            else:
                                file_entry['size_compare'] = "unknown"

                            simplejson.dump(file_entry, json_file)
                            json_file.write("\n")

    # download a key from a bucket 
    def download_file(self, conn, which_bucket, download_key_name):
        # download the file to local storage
        file_key = self.get_file_key(conn, which_bucket, download_key_name)
        file_name = file_key.name.split('/')[1]
        m = md5.new()

        # download the file
        with open(file_name, "w") as temp_file:
            bytes_requested = 10485760
            chunk_read = file_key.read(size=bytes_requested)
            total_transfer = 0
            while len(chunk_read):
                m.update(chunk_read) 
                temp_file.write(chunk_read)
                total_transfer = total_transfer + len(chunk_read)
                chunk_read = file_key.read(size=bytes_requested)

        return { 'md5_sum': m.hexdigest(), 'bytes_transferred': total_transfer }

