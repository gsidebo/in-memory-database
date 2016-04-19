from __future__ import print_function
from utils import Stack


class InMemoryDatabase(object):
    """Stores key-value pairs and maintains an index of current value counts in memory.

    Attributes:
        data: Internal storage for key-value pairs. Values are lists which act as a 
            value's history. Only one value can be 'active' in these lists.
        value_index: Dictionary which maps each active value in the data to the number
            of times it appears. This is maintained to reduce lookup times when 
            determining the number of keys that have a certain value.
    """

    def __init__(self):
        self.data = {}
        self.value_index = {}
    
    def increment_value_index(self, value):
        value_count = self.value_index.get(value, 0)
        self.value_index[value] = value_count + 1
        
    def decrement_value_index(self, value):
        self.value_index[value] -= 1
        if not self.value_index[value]:
            del self.value_index[value]
            
    def is_set(self, var):
        return var in self.data
    
    def get(self, var):
        return None if not self.is_set(var) else self.data[var].current()
    
    def add_or_replace(self, var, value, new=True):
        """
        Adds a new value which will act as the current value at key 'var', 
        or replaces the current value. Updates the value index accordingly.
        """
        if new and not self.is_set(var):
            self.data[var] = Stack()
        else:
            current_value = self.get(var)
            if current_value == value:
                # Setting the value to its existing value should have no effect
                return None
            else:
                # The current value is being changed, so the value index needs to be changed
                self.decrement_value_index(current_value)
        if not new:
            self.data[var].pop()
        self.data[var].push(value)
        self.increment_value_index(value)
        
    def add(self, var, value):
        self.add_or_replace(var, value, new=True)
    
    def change(self, var, value):
        self.add_or_replace(var, value, new=False)
        
    def remove(self, var):
        value = self.data[var].current()
        self.data[var].pop()
        if self.data[var].is_empty():
            del self.data[var]
        else:
            self.increment_value_index(self.get(var))
        self.decrement_value_index(value)
    
    def flatten(self):
        """
        Sets each value list in the data to contain only the active value, effectively
        deleting the 'history' of each value list.
        """
        for var in self.data.keys():
            self.data[var] = Stack(self.data[var].current())
            
    def num_equal_to(self, value):
        return self.value_index.get(value, 0)
    
    def __repr__(self):
        return 'Data: {}\nValue Index: {}'.format(self.data, self.value_index)

    
class DbSession(object):
    """Provides an API for users to make changes to an in-memory database with transactions.

    Attributes:
        database: An instance of an in-memory database.
        transaction_stack: A stack of active transactions.
        current_trans: The currently active transaction. Transactions are a set of keys which
            represent keys in the database that have been edited during the current transaction.
    """
    def __init__(self):
        self.database = InMemoryDatabase()
        self.transaction_stack = Stack()
        self.current_trans = None
        self.reset_transaction_state()
    
    def reset_transaction_state(self):
        self.current_trans = set() if self.transaction_stack.is_empty() else self.transaction_stack.current()
        # Transaction stack should always have a 'base' transaction which can't be rolled back/commited
        self.transaction_stack = Stack(self.current_trans)
        
    def pop_transaction(self):
        self.transaction_stack.pop()
        self.current_trans = self.transaction_stack.current()

    def has_open_transaction(self):
        return self.transaction_stack.size() > 1
        
    def begin(self):
        self.current_trans = set()
        self.transaction_stack.push(self.current_trans)
        
    def rollback(self):
        if not self.has_open_transaction():
            print('NO TRANSACTION')
        else:
            map(self.database.remove, list(self.current_trans))
            self.pop_transaction()
        
    def commit(self):
        if not self.has_open_transaction():
            print('NO TRANSACTION')
        else:
            self.database.flatten()
            self.reset_transaction_state()

    def set_var(self, var, value):
        if var in self.current_trans:
            self.database.change(var, value)
        else:
            self.database.add(var, value)
            self.current_trans.add(var)
    
    def unset_var(self, var):
        self.set_var(var, None)
            
    def get_var(self, var):
        print(self.database.get(var) or 'NULL')
    
    def num_equal_to(self, value):
        print(self.database.num_equal_to(value))

    def __repr__(self):
        return '{}\nTransaction Stack: {}'.format(self.database, self.transaction_stack)