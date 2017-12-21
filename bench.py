#!/usr/bin/env python3

import os
import sys
from threading import Thread, Lock
import psycopg2
import argparse
import logging

logger = logging.getLogger('benchmark')
mutex = Lock()

class App():

    benchmark_results = list()

    def __init__(self):
        parser = argparse.ArgumentParser(description='PostgreSQL benchmark')
        parser.add_argument('--db', type=str, required=True)
        parser.add_argument('--threads', type=str, default='1')
        parser.add_argument('--count', type=int, default=100000)
        parser.add_argument('--test', type=str, required=True)
        self.args = parser.parse_args()
        self.dir = os.path.join('./tests/', self.args.test)

    def exec_query(self, conn, query):
        cursor = conn.cursor()
        cursor.execute(query)
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]
        else:
            result = None
        cursor.close()
        return result

    def exec_script(self, conn, filename):
        logging.info('Executing script %s' % filename)
        query = open(filename, 'r').read()
        self.exec_query(conn, query)

    def get_version(self):
        conn = psycopg2.connect(self.args.db)
        result = self.exec_query(conn, 'SELECT version()')
        conn.close()
        return result

    def get_sorted_file_list(self, dir):
        if not os.path.isdir(dir):
            return []
        files = os.listdir(dir)
        files.sort()
        return [os.path.join(dir, file) for file in files]

    def execute_scripts_in_dir(self, dir):
        conn = psycopg2.connect(self.args.db)

        for script in self.get_sorted_file_list(os.path.join(self.dir, dir)):
            self.exec_script(conn, script)

        conn.commit()
        conn.close()

    def prepare_database(self):
        logging.info('Preparing database')
        logging.info('Database version: %s' % self.get_version())
        self.execute_scripts_in_dir('prepare')

    def prepare_data(self):
        files = self.get_sorted_file_list(os.path.join(self.dir, 'data'))

        conn = psycopg2.connect(self.args.db)
        cursor = conn.cursor()

        for file in files:
            logging.info('Loading data from file %s' % file)
            f = open(file)
            cursor.copy_from(f, os.path.basename(file).replace('.sql', ''))
            conn.commit()
        cursor.close()
        conn.close()

    def cleanup_database(self):
        self.execute_scripts_in_dir('cleanup')

    def run_test(self, i, count):
        conn = psycopg2.connect(self.args.db)

        cursor = conn.cursor()

        cursor.execute('select pg_backend_pid()')
        pid = cursor.fetchone()[0]
        logging.info('Starting thread %s, %s function calls, PID: %s' % (i, count, pid))

        query = open(os.path.join(self.dir, 'test.sql'), 'r').read()
        cursor.execute(query % {'count': count})

        result = cursor.fetchone()
        result_in_ms = (result[0]/count)*1000.0
        logging.info('RESULT: %.4f ms, %d TPS' % (round(result_in_ms, 4), 1/(result[0]/count)))
        mutex.acquire()
        self.benchmark_results.append(result_in_ms)
        mutex.release()

        conn.close()

    def run_tests(self, threads_count):
        logger.info('Running test in %s threads' % threads_count)
        self.threads = list()
        self.benchmark_results = list()

        for i in range(0, threads_count):
            thread = Thread(target=self.run_test, args=(i, self.args.count))
            self.threads.append(thread)
            thread.start()

        for thread in self.threads:
            thread.join()

        logging.info('Average result: %sms' % round(sum(self.benchmark_results)/len(self.benchmark_results), 3))

    def run(self, args):
        self.prepare_database()
        self.prepare_data()

        for threads_count in self.args.threads.split(','):
            self.run_tests(int(threads_count))

        self.cleanup_database()

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO, stream=sys.stdout)
    app = App()
    sys.exit(app.run(sys.argv))
