from pyspark import SparkContext, SparkConf

INPUT_FILE_PATH = 'hdfs://hdfsmaster02/tmp/eric_test_data/2015_07_22_mktplace_shop_web_log_sample.log.gz'


def setup_configuration():
    conf = SparkConf() \
           .setAppName('Web Log Challenge') \
           .setMaster('spark://sparkmaster01.internal.chango.com:7077') \
           .set('spark.cores.max', '100') \
           .set('spark.executor.memory', '4g')
    return conf


def log_mapping(iterator):
    '''
    Maps the log to field names obtained from the AWS Elastic Load Balancer
    documentation. Returns the mapped results as a dictionary
    '''
    import shlex
    fieldnames = ('timestamp', 'elb', 'client:port', 'backend:port',
                  'request_processing_time', 'backend_processing_time',
                  'response_processing_time', 'elb_status_code',
                  'backend_status_code', 'received_bytes', 'sent_bytes',
                  'request', 'user_agent', 'ssl_cipher', 'ssl_protocol')
    for parts in iterator:
        if parts:
            try:
                data_list = shlex.split(parts)
                line_dict = dict(zip(fieldnames, data_list))
            except:
                continue
            yield line_dict


def time_window_extraction(iterator):
    '''
    Extracts the time window from the timestamp. The time window is
    assumed to be 15 minutes and for each time interval, it will
    label what the time window will be
    '''
    for parts in iterator:
        time_window_list = parts.get('timestamp').split('T')[1].split(':')
        hour = int(time_window_list[0])
        minute = int(time_window_list[1])
        if minute >= 0 and minute <= 15:
            parts['minute_time_window'] = '0-15 minutes'
        elif minute > 15 and minute <= 30:
            parts['minute_time_window'] = '15-30 minutes'
        elif minute > 30 and minute <= 45:
            parts['minute_time_window'] = '30-45 minutes'
        elif minute > 45 and minute <= 59:
            parts['minute_time_window'] = '45-60 minutes'
        parts['hour'] = hour
        yield parts


def page_hit_count(iterator):
    '''
    Maps the hour, minute_time_window, the IP and a count of 1
    '''
    for parts in iterator:
        if parts.get('client:port'):
            parts['client_ip'] = parts.get('client:port').split(':')[0]
            yield ((parts.get('hour'), parts.get('minute_time_window'),
                    parts.get('client_ip')), 1)


def time_per_session(iterator):
    '''
    NOTE: The session length is assumed to be 15 minutes

    To calculate the session time per view, the number of views needs to be 
    divided by 15 minutes which is the assumed session length.
    The session will be mapped along with the session_time for it

    The session will be labelled with the minute_time_window:
    [0 - 15 minutes, 15 - 30 minutes, 30 - 45 minutes, 45 - 60 minutes]

    The IP of the user is not taken into consideration for this
    '''
    for parts in iterator:
        session = parts[0][1]
        view = parts[1]
        session_length = 15
        session_time = float(session_length/view)
        yield (session, session_time)


def session_time_per_ip(iterator):
    '''
    NOTE: The session length is assumed to be 15 minutes

    To calculate the session time per ip, the number of views/visits needs
    to be divided by 15 minutes which is the assumed session length.
    The ip will be mapped along with the session_time for it
    '''
    for parts in iterator:
        ip = parts[0][2]
        view = parts[1]
        session_length = 15
        session_time = float(session_length/view)
        yield (ip, session_time)


def urls_per_session(iterator):
    '''
    Maps the url per session. The session is obtained from the
    minute_time_window
    '''
    for parts in iterator:
        session = parts.get('minute_time_window')
        url = parts.get('request').split()[1]
        yield (session, url)


def compare_session_time(session_time_one, session_time_two):
    '''
    Compares the session time. The session time with the greater time
    is returned
    '''
    if session_time_one > session_time_two:
        return session_time_one
    else:
        return session_time_two


def main():

    conf = setup_configuration()
    sc = SparkContext(conf=conf)
    try:
        web_log_rdd = sc.textFile(INPUT_FILE_PATH).mapPartitions(log_mapping). \
                      repartition(100).mapPartitions(time_window_extraction).cache()

        # Sessionize the weblog to obtain the number of page hits by visitor/IP
        # during a 15 minute time window per hour 
        sessionize = web_log_rdd.mapPartitions(page_hit_count). \
                     reduceByKey(lambda x, y: x + y).cache()
        sessionize.coalesce(1).saveAsTextFile('hdfs://hdfsmaster02/tmp/eric_test_data/sessionize_output.txt')

        # Calculate the average session time per view
        session = sessionize.mapPartitions(time_per_session).cache()
        session_count = sc.broadcast(session.countByKey())
        session = session.reduceByKey(lambda x, y: x + y)
        average_session_time = session.map(lambda x: (x[0], x[1]/session_count.value[x[0]]))
        average_session_time.coalesce(1).saveAsTextFile('hdfs://hdfsmaster02/tmp/eric_test_data/average_session_time.txt')

        # Determine the unique URL visits per session
        unique_url_visits = web_log_rdd.mapPartitions(urls_per_session).countApproxDistinct()
        print unique_url_visits

        # IPs with the longest session times
        ip_with_session_time = sessionize.mapPartitions(session_time_per_ip).reduceByKey(compare_session_time)
        ip_with_session_time.coalesce(1).saveAsTextFile('hdfs://hdfsmaster02/tmp/eric_test_data/ip_with_session_time.txt')

    except:
        pass

if __name__ == '__main__':
    main()
