#! /usr/bin/env python3.3
'''AccessQueue'''

from collections import deque

# pylint: disable=C0103
# Invalid name
class AccessQueue:
    '''
    A queue where the least-recently accessed object is at index 0, most
    recently accessed object is at the end of the queue.
    '''
    def __init__(self, maxlen=None):
        self.q = deque(maxlen=maxlen)

    def access(self, obj):
        '''
        Record that an object has been accessed, move to back of the queue.

        Return object that fell off the front of our queue if we just
        exceeded our maxlen. You might want to recycle or delete the
        returned object.

        Return None if nothing fell off the queue.
        '''
        prev_first = None
        if self.q:
            prev_first = self.q[0]

        if obj in self.q:
            self.q.remove(obj)

        self.q.append(obj)

        if (prev_first != self.q[0]
            and prev_first != obj):
            return prev_first

        return None

    def pop_oldest(self):
        '''
        Remove the least-recently-accessed object from our queue and return it.
        '''
        return self.q.popleft()

    def is_full(self):
        '''
        Are we at capacity, will a call to access() of something not in our queue
        cause an object to fall off?
        '''
        return self.q.maxlen == len(self.q)

    def clear(self):
        '''
        Reset to empty.
        '''
        self.q.clear()

