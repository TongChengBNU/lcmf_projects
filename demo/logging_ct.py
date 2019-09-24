import logging
logging.basicConfig(filename='logging_ct.log', level=logging.DEBUG, format='%(asctime)s %(message)s')

logger = logging.getLogger(__name__)


logging.debug('Debug')
logging.info('Info')
logging.warning('Warning')
logging.error('Error')
logging.critical('Critical')
