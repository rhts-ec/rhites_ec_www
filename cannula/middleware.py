from django.contrib.auth import logout
from django.conf import settings

from datetime import datetime

class RevokeAuthenticationMiddleware:
    def process_view(self,request,view_func,view_args,view_kwargs):
        if request.user.is_authenticated() and not request.user.is_active:
            logout(request)

class RequestActivityTimeoutMiddleware:
    def process_view(self,request,view_func,view_args,view_kwargs):
        if not request.user.is_authenticated():
            return # ignore anonymous accounts

        curr_timestamp = datetime.now()
        if 'request_activity.last_request' in request.session:
            try:
                prev_timestamp = datetime.fromtimestamp(request.session['request_activity.last_request'])
                # prev_timestamp = datetime.strptime(request.session['request_activity.last_request'], '%Y-%m-%dT%H:%M:%S.%f')
                activity_delta = curr_timestamp - prev_timestamp
                if activity_delta.seconds > settings.INACTIVITY_TIMEOUT:
                    logout(request)
            except TypeError:
                logout(request) # something went wrong, logout and reset
        
        request.session['request_activity.last_request'] = curr_timestamp.timestamp()
        # request.session['request_activity.last_request'] = curr_timestamp.isoformat()
