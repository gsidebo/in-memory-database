from __future__ import print_function


class InMemoryDatabase(object):
    """Stores key-value pairs and maintains an index of current value counts in memory

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
        return None if not self.is_set(var) else self.data[var][0]
    
    def add_or_replace(self, var, value, new=True):
        """
        Adds a new value which will act as the current value at key 'var' or
        replaces the current value. Updates the value index accordingly.
        """
        if new and not self.is_set(var):
            self.data[var] = []
        else:
            current_value = self.get(var)
            if current_value == value:
                # Setting the value to its existing value should have no effect
                return None
            else:
                # The current value is being changed, so the value index needs to be changed
                self.decrement_value_index(current_value)
        if new:
            self.data[var].insert(0, value)
        else:
            self.data[var][0] = value
        self.increment_value_index(value)
        
    def add(self, var, value):
        self.add_or_replace(var, value, new=True)
    
    def change(self, var, value):
        self.add_or_replace(var, value, new=False)
        
    def remove(self, var):
        value = self.data[var][0]
        self.data[var].pop(0)
        if not self.data[var]:
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
            self.data[var] = [self.data[var][0]]
            
    def num_equal_to(self, value):
        return self.value_index.get(value, 0)
    
    def __repr__(self):
        return 'Data: {}\nValue Index: {}'.format(self.data, self.value_index)

    
class DbSession(object):
    """Stores key-value pairs and maintains an index of current value counts in memory

    Attributes:
        data: Internal storage for key-value pairs. Values are lists which act as a 
            value's history. Only one value can be 'active' in these lists.
        value_index: Dictionary which maps each active value in the data to the number
            of times it appears. This is maintained to reduce lookup times when 
            determining the number of keys that have a certain value.
    """
    def __init__(self):
        self.database = InMemoryDatabase()
        self.transaction_stack = []
        self.reset_transaction_state()
    
    def reset_transaction_state(self):
        self.current_trans = set() if not self.transaction_stack else self.transaction_stack[0]
        # Transaction stack will always have a 'base' transaction - cannot be rolled back/commited
        self.transaction_stack = [self.current_trans]
        
    def pop_transaction(self):
        self.transaction_stack.pop(0)
        self.current_trans = self.transaction_stack[0]

    def has_open_transaction(self):
        return len(self.transaction_stack) > 1
        
    def begin(self):
        self.current_trans = set()
        self.transaction_stack.insert(0, self.current_trans)
        
    def rollback(self):
        if not self.has_open_transaction():
            print('NO TRANSACTION')
        else:
            for var in list(self.current_trans):
                self.database.remove(var)
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