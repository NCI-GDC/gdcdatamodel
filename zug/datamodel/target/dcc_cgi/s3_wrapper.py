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
        for env in os.environ.keys():
            if env.find("ACCESS_KEY") != -1:
                if env.find("CEPH") != -1:
                    print "ceph access key from", env
                    self.s3_inst_info['ceph']['access_key'] = os.environ[env]
                elif env.find("CLEVERSAFE") != -1:
                    print "cleversafe access key from", env
                    self.s3_inst_info['cleversafe']['access_key'] = os.environ[env]
                else:
                    print "ceph/cleversafe access key from", env
                    self.s3_inst_info['ceph']['access_key'] = os.environ[env]
                    self.s3_inst_info['cleversafe']['access_key'] = os.environ[env]
            if env.find("SECRET_KEY") != -1:
                if env.find("CEPH") != -1:
                    print "ceph secret key from", env
                    self.s3_inst_info['ceph']['secret_key'] = os.environ[env]
                elif env.find("CLEVERSAFE") != -1:
                    print "cleversafe secret key from", env
                    self.s3_inst_info['cleversafe']['secret_key'] = os.environ[env]
                else:
                    print "ceph/cleversafe secret key from", env
                    self.s3_inst_info['ceph']['secret_key'] = os.environ[env]
                    self.s3_inst_info['cleversafe']['secret_key'] = os.environ[env]
                
        #with open("s3_conf.yaml") as s3_file:
        #    self.s3_settings = yaml.load(s3_file)
        #self.s3_inst_info = self.s3_settings['S3_INSTS']
        #print self.s3_inst_info 
        #for key, value in self.s3_inst_info.iteritems():
        #    if 'auth_path' in value:
        #        with open(os.path.expanduser(value['auth_path'])) as f:
        #            s3_auth = yaml.load(f.read().strip())
        #            value['access_key'] = s3_auth['access_key']
        #            value['secret_key'] = s3_auth['secret_key']
        #    else:
        #        if key.lower() == "ceph":
        #            value['access_key'] = os.environ["S3_ACCESS_KEY"]
        #            value['secret_key'] = os.environ["S3_SECRET_KEY"]
        #        if key.lower() == "cleversafe":
        #            value['access_key'] = os.environ["S3_ACCESS_KEY"]
        #            value['secret_key'] = os.environ["S3_SECRET_KEY"]

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

    def check_s3(self, which_s3, url_offset = 0):
        s3_ok = False
        if type(self.s3_inst_info[which_s3]['url']) == list:
            if url_offset >= len(self.s3_inst_info[which_s3]['url']):
                print "Invalid offset, using 0"
                url_offset = 0
            host_url = self.s3_inst_info[which_s3]['url'][url_offset]
        else:
            host_url = self.s3_inst_info[which_s3]['url']
        print "Checking %s" % which_s3
        if which_s3 in self.s3_inst_info:
            logger.info('Checking that s3 is reachable')
            print "Checking %s" % format(host_url)
            if self.s3_inst_info[which_s3]['secure']:
                r = requests.get('https://{}'.format(host_url))
            else:
                r = requests.get('http://{}'.format(host_url))

            if r.status_code != 200:
                logging.error('Status: {}'.format(r.status_code))
                raise Exception('s3 unreachable at {}'.format(host_url))
            else:
                print "Connect ok"
                s3_ok = True
                logger.info('Found s3 gateway at {}'.format(host_url))

        return s3_ok

    def connect_to_s3(self, which_s3, url_offset = 0):
        if which_s3 in self.s3_inst_info:
            if type(self.s3_inst_info[which_s3]['url']) == list:
                host_url = self.s3_inst_info[which_s3]['url'][url_offset]
            else:
                host_url = self.s3_inst_info[which_s3]['url']
            print "Connecting to %s - %s" % (which_s3, host_url)
            conn = boto.connect_s3(
                aws_access_key_id=self.s3_inst_info[which_s3]['access_key'],
                aws_secret_access_key=self.s3_inst_info[which_s3]['secret_key'],
                host=self.s3_inst_info[which_s3]['url'],
                is_secure=self.s3_inst_info[which_s3]['secure'],
                calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                )
        return conn

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

        print "Creating key:", key_name
        new_key.key = key_name
        new_key.set_contents_from_filename(file_name)

    def upload_multipart_file(self, conn, bucket_name, key_name, file_iterator, calc_md5=False, save_local=False):
        print "Uploading to key %s, bucket %s" % (key_name, bucket_name)
        self.md5_sum = md5.new()
        bucket_exists = False
        if 'https_proxy' in os.environ:
            del os.environ['https_proxy']
        if 'http_proxy' in os.environ:
            del os.environ['http_proxy']
        print "Checking buckets"
        bucket_exists = False
        try:
            bucket_list = conn.get_all_buckets()
        except:
            print "Unable to list buckets:", sys.exc_info()[1]
        else:
            for instance in bucket_list:
                if instance.name == bucket_name:
                    bucket_exists = True
                    break

        if bucket_exists:
            bucket = conn.get_bucket(bucket_name)
        else:
            bucket = conn.create_bucket(bucket_name)

        print "Creating stream"
        stream_buffer = BIO()
        mp_chunk_size = 1073741824 # 1GiB
        cur_size = 0
        chunk_index = 1
        total_size = 0
        print "Initiating multipart upload"
        mp = bucket.initiate_multipart_upload(key_name)
        start_time = time.clock()

        for chunk in file_iterator:
#            print len(chunk)
            size_info = self.get_nearest_file_size(total_size)
            cur_time = time.clock()
            base_transfer_rate = float(total_size) / float(cur_time - start_time)
            transfer_info = self.get_nearest_file_size(base_transfer_rate)
            cur_conv_size = float(total_size) / float(size_info[0])
            cur_conv_rate = base_transfer_rate / float(transfer_info[0])
            sys.stdout.write("%7.02f %s : %6.02f %s per sec\r" % (
                cur_conv_size, size_info[1],
                cur_conv_rate, transfer_info[1]))
            #sys.stdout.write("%15d - %15d\r" % (total_size, cur_size))
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
                    print "Saving local copy"
                    with open("tmp_" + key_name.split('/')[1], "wb") as save_file:
                        for chunk in stream_buffer:
                            save_file.write(chunk)
                    stream_buffer.seek(0)
                try:
                    print "\nWriting multipart chunk %d" % chunk_index
                    result = mp.upload_part_from_file(stream_buffer, chunk_index)
                except:
                    print "Error writing %d bytes" % cur_size
                    print sys.exc_info()[1]
                    sys.exit()
                else:
                    #print result
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
            print "\nWriting final multipart chunk %d, size %d" % (
                chunk_index, len(stream_buffer)
            )
            result = mp.upload_part_from_file(stream_buffer, chunk_index)
        except:
            print "Error writing %d bytes" % cur_size
            print sys.exc_info()[1]
            sys.exit()
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
        print "Complete, %7.02f %s : %6.02f %s per sec\r" % (
            cur_conv_size, size_info[1],
            cur_conv_rate, transfer_info[1])

        if cur_size > 0:
            os.environ['http_proxy'] = "http://cloud-proxy:3128"
            os.environ['https_proxy'] = "http://cloud-proxy:3128"
            stream_buffer.seek(0)
            try:
                result = mp.upload_part_from_file(stream_buffer, chunk_index)
            except:
                print "Error writing %d bytes" % cur_size

        mp.complete_upload()
        print "Upload complete, md5 = %s, %d bytes transferred" % (str(self.md5_sum.hexdigest()), total_size)
        return {"md5_sum" : str(self.md5_sum.hexdigest()), "bytes_transferred" : total_size }

        #print dir(stream_buffer)
        #stream_buffer.seek(0, os.SEEK_END)
        #print stream_buffer.tell()
        #stream_buffer.seek(0, os.SEEK_START)
        



    def get_count_of_keys_in_s3_bucket(self, conn, bucket_name):
        bucket = conn.get_bucket(bucket_name)
        rs = bucket.list()
        file_count = 0
        for key in rs:
            file_count = file_count + 1

        return file_count

    def get_file_key(self, conn, bucket_name, file_name):
#        return_key = ""
        key = None
        try:
            bucket = conn.get_bucket(bucket_name)
        except:
            type1, value1, traceback1 = sys.exc_info()
            print "Unable to get bucket %s, error %s" % (bucket_name, sys.exc_info()[1])
        else:
            key = bucket.get_key(file_name)
        return key
#        rs = bucket.list()
#        anims = [ '/', '|', '\\', '-' ]
#        counter = 0
#        for key in rs:
#            sys.stdout.write("%05d\r" % counter)
            #sys.stdout.write("%c\r" % (anims[counter % len(anims)]))
#            sys.stdout.flush()
#            counter = counter + 1
#            if key.name == file_name:
#                return_key = key
#                break
#        return return_key
    
    def delete_file_key(self, conn, bucket_name, file_name):
        key = self.get_file_key(conn, bucket_name, file_name)
        key.delete()


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
                print "Warning, file %s has zero size" % key.name
                zero_size_files = zero_size_files + 1
            if key.size != data.size:
                print "Warning, file %s has size mismatch: %d - key, %d - list" % (key.name, data.size, key.size)
                mismatched_files = mismatched_files + 1

        print "%d zero length files, %d mismatched file sizes" % (zero_size_files, mismatched_files)
        
        return file_list



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
            #sys.stdout.write("%c\r" % animation[animation_index % len(animation)])
            format_str = "%%0%dd/%%0%dd\r" % (digits, digits)
            sys.stdout.write(format_str % (file_count, num_files))
            sys.stdout.flush()
            animation_index = animation_index + 1
            if key.size == 0:
                print "Warning, file %s has zero size" % key.name
                zero_size_files = zero_size_files + 1
            else:
                if check_download:
                    try:
                        data = key.read(size=1024)
                    except:
                        print "Warning, file %s cannot be read" % key.name
                        unreachable_files = unreachable_files + 1

        return file_list

    def get_s3_file_counts(self, conn, which_s3):
        if self.check_s3(which_s3):
            rs = conn.get_all_buckets()
            print "%d buckets in S3 container" % len(rs)
            self.all_files = {}
            for entry in rs:
                print entry.name
                if entry.name in self.allowed_buckets:
                    self.all_files[entry.name] = self.get_files_in_s3_bucket(conn, entry.name)
            self.check_time = datetime.datetime.now()

        else:
            print "Unable to connect to %s" % s3_url

    def save_s3_file_lists(self, which_s3):
        #graph_db = self.connect_to_neo4j()
        for key, value in self.all_files.iteritems():
            bucket_filename = "%s_%s_files_%04d-%02d-%02d_%02d-%02d-%02d.json" % (which_s3, key, datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day, datetime.datetime.now().hour, datetime.datetime.now().minute, datetime.datetime.now().second)
            with open(bucket_filename, "w") as json_file:
                print "making psqlgraph connection"
                psql_test = Psqlgraph_Test()
                with psql_test.driver.session_scope() as session:
                    for entry in value:
                        file_entry = {}
                        file_entry['file_name'] = entry[0]
                        #print "file name: ", file_entry['file_name']
                        #sys.exit()
                        file_entry['file_size'] = int(entry[1])
                        if len(file_entry['file_name'].split("/")) > 1:
                            file_key = file_entry['file_name'].split("/")[0]
                            file_name = file_entry['file_name'].split("/")[1]
                            #print "checking psqlgraph for md5 sum for %s" % file_entry['file_name']
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

    def download_file(self, conn, which_bucket, download_key_name):
        # download the file to local storage
        file_key = self.get_file_key(conn, which_bucket, download_key_name)
        file_name = file_key.name.split('/')[1]
        print "Downloading %s, size %d" % (file_key.name, file_key.size)
        m = md5.new()
        # download the file
        with open(file_name, "w") as temp_file:
            bytes_requested = 10485760
            chunk_read = file_key.read(size=bytes_requested)
            total_transfer = 0
    #        while total_transfer < file_key.size:
    #            if len(chunk_read):
            while len(chunk_read):
                m.update(chunk_read) 
                #sys.stdout.write("%d/%d\r" % (total_transfer, file_key.size))
                #sys.stdout.flush()
                temp_file.write(chunk_read)
                total_transfer = total_transfer + len(chunk_read)
                chunk_read = file_key.read(size=bytes_requested)
                #else:
            #    print "0 bytes read"

        return { 'md5_sum': m.hexdigest(), 'bytes_transferred': total_transfer }

    def check_file_list(self, conn, which_s3, files, count=1):
        max_file_size = 102400000000000
        results = []
        graph_db = self.connect_to_neo4j()
        for i in range(count):
            print "Pass %d/%d" % ((i + 1), count)
            for file in files:
                print file
                result = self.verify_file(conn, file, graph_db, max_file_size)
                results.append(result)
                print
        return results


