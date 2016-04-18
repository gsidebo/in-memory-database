from __future__ import print_function
import sys
from database import DbSession


# Maps a command key to the name of a DbSession function
COMMAND_MAP = {
    'BEGIN': 'begin',
    'COMMIT': 'commit',
    'ROLLBACK': 'rollback',
    'SET': 'set_var',
    'UNSET': 'unset_var',
    'GET': 'get_var',
    'NUMEQUALTO': 'num_equal_to'
}


def main():
    db_session = DbSession()
    while True:
        try:
            user_input = raw_input()
            cmd_parts = user_input.split(' ')
            if cmd_parts[0]:
                if cmd_parts[0] == 'END':
                    break
                # Determine the function to call based on the first part of the command
                fn_to_call = getattr(db_session, COMMAND_MAP[cmd_parts[0]])
                # Execute the function with the remaining parts of the command acting as arguments
                fn_to_call(*cmd_parts[1:])
        except KeyError as ex:
            print(
                'Command {} not recognized.\nAvailable commands: {}'.format(
                    ex[0], 
                    ', '.join(COMMAND_MAP.keys())
                )
            )
        except EOFError:
            break


if __name__ == "__main__":
    main()