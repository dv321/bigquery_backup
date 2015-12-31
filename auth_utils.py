from oauth2client.client import SignedJwtAssertionCredentials
import httplib2

thekey = 'your private key'

def authorize_service_account(scope):
    """Prepare oAuth2 credentials for a service account.

    Returns:
        An authorized httplib2.Http() instance.
    """

    credentials = SignedJwtAssertionCredentials(
        service_account_name='your service account email address',
        private_key=thekey.encode('utf-8'),
        scope=scope
    )

    return credentials.authorize(httplib2.Http())
