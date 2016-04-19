class Stack(object):
    def __init__(self, *initial_items):
        self.items = list(initial_items)

    def is_empty(self):
        return len(self.items) == 0

    def push(self, item):
        self.items.append(item)

    def pop(self):
        return self.items.pop()

    def current(self):
        return self.items[-1]

    def size(self):
        return len(self.items)