import sys
import itertools
import json
import logging
try:
    import M2Crypto  # Required to check integrity of SNS HTTP notifications
except ImportError as error:
    M2Crypto = error

if sys.version_info < (3, ):
    # python2
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
    from urllib2 import urlopen
else:
    # python3
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.request import urlopen


class SNS_HTTPServer(HTTPServer):
    """ HTTP Server to receive SNS notifications via HTTP """
    def set_fs(self, fs):
        # Throw if some one attempts to use the http mode without M2Crypto
        if isinstance(M2Crypto, Exception):
            raise M2Crypto
        self.fs = fs


class SNS_HTTPRequestHandler(BaseHTTPRequestHandler):
    """ HTTP Request Handler to receive SNS notifications via HTTP """
    logger = logging.getLogger('yas3fs')

    def do_POST(self):
        if self.path != self.server.fs.http_listen_path:
            self.send_response(404)
            return

        content_len = int(self.headers.getheader('content-length'))
        post_body = self.rfile.read(content_len)

        message_type = self.headers.getheader('x-amz-sns-message-type')
        message_content = json.loads(post_body)

        # Check SNS signature, I was not able to use boto for this...

        url = message_content['SigningCertURL']
        if not hasattr(self, 'certificate_url') or self.certificate_url != url:
            self.logger.debug('downloading certificate')
            self.certificate_url = url
            self.certificate = urlopen(url).read()

        signature_version = message_content['SignatureVersion']
        if signature_version != '1':
            self.logger.debug('unknown signature version')
            self.send_response(404)
            return

        signature = message_content['Signature']

        del message_content['SigningCertURL']
        del message_content['SignatureVersion']
        del message_content['Signature']
        if 'UnsubscribeURL' in message_content:
            del message_content['UnsubscribeURL']
        string_to_sign = '\n'.join(list(itertools.chain.from_iterable(
                    [(k, message_content[k]) for k in sorted(message_content.keys())]
                    ))) + '\n'

        cert = M2Crypto.X509.load_cert_string(self.certificate)
        pub_key = cert.get_pubkey().get_rsa()
        verify_evp = M2Crypto.EVP.PKey()
        verify_evp.assign_rsa(pub_key)
        verify_evp.reset_context(md='sha1')
        verify_evp.verify_init()
        verify_evp.verify_update(string_to_sign.encode('ascii'))

        if verify_evp.verify_final(signature.decode('base64')):
            self.send_response(200)
            if message_type == 'Notification':
                message = message_content['Message']
                self.logger.debug('message = %s' % message)
                self.server.fs.process_message(message)
            elif message_type == 'SubscriptionConfirmation':
                token = message_content['Token']
                response = self.server.fs.sns.confirm_subscription(self.server.fs.sns_topic_arn, token)
                self.server.fs.http_subscription = response['ConfirmSubscriptionResponse']['ConfirmSubscriptionResult']['SubscriptionArn']
                self.logger.debug('SNS HTTP subscription = %s' % self.server.fs.http_subscription)
            else:
                self.logger.debug('unknown message type')
            return
        else:
            self.logger.debug('wrong signature')

        # If nothing better, return 404
        self.send_response(404)

    def do_GET(self):
        self.logger.debug('http get')
        self.send_response(404)

    def do_HEAD(self):
        self.logger.debug('http head')
        self.send_response(404)
