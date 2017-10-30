#!/usr/bin/python -u
# Copyright 2017 Scraper Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
"""

import argparse
import datetime
import sys
from google.cloud import storage
from google.cloud import exceptions

def parse_cmdline(args):
    """Parse the commandline arguments.
    Args:
      args: the command-line arguments, minus the name of the binary
    Returns:
      the results of ArgumentParser.parse_args
    """
    parser = argparse.ArgumentParser(
        #parents=[oauth2client.tools.argparser],
        description='Generate tasks to reprocess files from GCS path '
                    'or date range.')
    parser.add_argument(
        '--project',
        metavar='PROJECT',
        type=str,
        default='mlab-oti',
        required=False,
        help='The project that owns the queues.')
    parser.add_argument(
        '--queue_prefix',
        metavar='QUEUE_PREFIX',
        type=str,
        default='etl-ndt-batch-',
        required=False,
        help='The prefix of the batch queues.')
    parser.add_argument(
        '--bucket',
        metavar='BUCKET',
        type=str,
        default='archive-mlab-oti',
        required=False,
        help='The Google Cloud Storage bucket to use')
    parser.add_argument(
        '--prefix',
        metavar='PREFIX',
        type=str,
        default='mlab-storage-scraper-test',
        required=False,
        help='The file prefix.') 
    parser.add_argument(
        '--start_date',
        metavar='START',
        type=lambda x: datetime.datetime.strptime(x, '%Y%m%d'),
        default='',
        required=False,
        help='Optional start date.')
    parser.add_argument(
        '--end_date',
        metavar='END',
        type=lambda x: datetime.datetime.strptime(x, '%Y%m%d'),
        default='00010101',
        required=False,
        help='Optional end date.')
    return parser.parse_args(args)

class ArchiveProcessor:
    """ ArchiveProcessor encapsulates the source bucket and prefix,
    and destination queue set or pubsub topic.
    """

    def __init__(self, bucket, prefix, queue):
        """
        Args:
           bucket:
           prefix:
           queue:
        """
        self.prefix = prefix
        self.queue = queue
        self.client = storage.Client()
        try:
            self.bucket = self.client.get_bucket(bucket)
        except exceptions.NotFound:
            print 'Oops no bucket', bucket
        raise 

    def post(tags, files):
        """ Post all tasks in list to the queue.
        Args:
          tags: map of tags to attach to the tasks.  The special tag
                "day" is used to choose the day of the week for
                batch task queues.
          file: list of filenames to post as tasks.
        Returns:

        """
        pass

    def one_day(date, files):
        """Add all tasks for a single day to appropriate queue.
        Args:
          date: a datetime.date indicating which date to post.
          files: list of file names to post.
        Returns:
          the results of ArgumentParser.parse_args
        """
        pass

def main(argv):
    print 'hello world'
    print datetime.datetime.strptime('20171025', '%Y%m%d')
    """Run scraper.py in an infinite loop."""
    args = parse_cmdline(argv[1:])

    try:
        processor = ArchiveProcessor(args.bucket, "", "")
        print processor.bucket
        it = processor.bucket.list_blobs(prefix=args.prefix, max_results=10)
        print list(it)
    except exceptions.NotFound:
        print 'Oops no bucket', args.bucket

if __name__ == '__main__':  # pragma: no cover
    main(sys.argv)
