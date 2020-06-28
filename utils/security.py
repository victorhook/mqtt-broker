import binascii
import hashlib
import getpass
import os
import sys

CREDENTIALS_FILE = 'passwd'
BASE_DIR    = os.path.join(os.path.dirname(sys.argv[0]), 'etc')
CREDENTIALS_PATH = os.path.join(BASE_DIR, CREDENTIALS_FILE)
PASSWORD_LIMIT   = 4

def hash_password(password):
    """ hashes a given password with sha256 and returns the
        hashed password together with the salt """

    # create a salt from random bytes
    salt = hashlib.sha256(os.urandom(64)).hexdigest().encode('ascii')
    # hash the password, using sha256
    hashed_pass = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 0xffff)
    # turn into hex
    hashed_pass = binascii.hexlify(hashed_pass)

    # put the salt as the first 64 bytes & return as ascii
    return (salt + hashed_pass).decode('ascii')

def verify_password(stored_password, password):
    """ checks wether the requested password matches the stored
        by hashing it with the same salt and comparing the result. 
        Returns True if OK                                          """

    # grab the salt part of the stored pass
    salt = stored_password[:64]
    # grab the password part
    correct_password = stored_password[64:]

    # hash the requested password with sha256
    hashed_pass = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'),
                                        salt.encode('ascii'), 0xffff)                      
    # turn into hex & ascii
    hashed_pass = binascii.hexlify(hashed_pass).decode('ascii')

    return correct_password == hashed_pass

def username_ok(username):
    """ username can't currently exist and has to be more than 1 letter """

    is_ok = True

    if len(username) < 2:
        print('Yo[*]  u can make a better username than that!')
        is_ok = False

    elif os.path.exists(CREDENTIALS_PATH):
        usernames = get_usernames()
        if username in usernames:
            print('So[*]  rry, that username is already taken, please choose a different one')
            is_ok = False
    
    return is_ok
        
def passwords_ok(password1, password2):
    """ passwords must match and be longer than PASSWORD_LIMIT """    

    is_ok = True
    if len(password1) < PASSWORD_LIMIT:
        print('Yo[*]  u should have a longer password...')
        is_ok = False
    elif password1 != password2:
        print('Pa[*]  sswords doens\'t match, try again')
        is_ok = False
    
    return is_ok

def delete_credentials():
    if os.path.exists(CREDENTIALS_PATH):
        os.remove(CREDENTIALS_PATH)

def get_usernames():
    """ opens the credential file and returns the usernames, should never return None """
    if os.path.exists(CREDENTIALS_PATH):
        with open(CREDENTIALS_PATH) as f:
            return [line.split(':')[0] for line in f.readlines()]
    return None



def delete_user(username):
    if os.path.exists(CREDENTIALS_PATH):
        with open(CREDENTIALS_PATH) as f:
            usernames = [line.split(':')[0] for line in f.readlines()]
            if username in usernames:
                pass


def save_new_credentials(username, password):
    with open(CREDENTIALS_PATH, 'w') as f:
        f.write(f'{username}:{hash_password(password)}\n')

def setup_credentials():

    answer = ''
    if os.path.exists(CREDENTIALS_PATH):
        while answer != 'yes' and answer != 'no':
            answer = input('[*]  You already have credentials set up,\n' + \
                    'Do you want to delete the old one and make a new? yes/no ').lower()

            if answer.startswith('y'):
                delete_credentials()
            
            elif answer.startswith('n'):
                return

    print('\n[*]  Setup new credentials. Please enter your requested Username and Password: ')

    username = None
    while username is None:
        username = input('[*]  Username: ')
        if not username_ok(username):
            username = None


    password_ok = False
    while not password_ok:
        password1  = getpass.getpass('[*]  Password: ')
        password2 = getpass.getpass('[*]  Re-type Password: ')

        if passwords_ok(password1, password2):
            password_ok = True
        
    save_new_credentials(username, password1)
    del username, password1, password2

    print('\n[*]  Credentials saved, you\'re ready to go!')
    print('[*]  These credentials will now be compared with all incoming messages')
    

## TODO : delete_user(), add_user(), argparse
