#!/usr/bin/env python2.7
import traceback
from graylogger import GrayLogger

API_URL = 'http://development.digitaldemocracy.org:12202/gelf'

def create_payload(db, table, affected_rows):
  return {
    '_db': db,
    '_table': table,
    '_affected_rows': affected_rows
  }

def main(logger):
  # Do some stuff...
  payload = create_payload('DDDB', 'Bill', 12)
  logger.info('Insert Into DDDB', additional_fields=payload)

  try:
    # We did something bad -- but we will catch it like good programmers.
    raise ValueError()
  except ValueError:
    logger.warning(
      'Insert failed',
      full_msg=traceback.format_exc(),
      additional_fields=create_payload('DDDB', 'ActionExtract', 0)
    )

  # Uncaught exception. This will be automatically logged because
  # we used GrayLogger in a with statement.
  raise TypeError()

if __name__ == '__main__':
  with GrayLogger(API_URL) as logger:
    main(logger)
