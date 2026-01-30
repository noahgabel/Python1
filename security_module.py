"""
Security Module - Kryptografi
"""

import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
import base64


class SecurityManager:
    """Håndterer kryptering og dekryptering af data"""

    def __init__(self, key=None):
        if key is None:
            self.key = os.urandom(32)
        else:
            self.key = key

    def get_key(self):
        return self.key

    # METHOD 1: AES-GCM
    def encrypt_aes_gcm(self, data):
        """
        Krypter med AES-GCM

        Fordele: AEAD, ingen padding, hurtig, modstandsdygtig mod manipulation
        Ulemper: Nonce skal være unik
        """
        aesgcm = AESGCM(self.key)
        nonce = os.urandom(12)

        if isinstance(data, str):
            data = data.encode('utf-8')

        ciphertext = aesgcm.encrypt(nonce, data, None)
        return base64.b64encode(nonce + ciphertext).decode('utf-8')

    def decrypt_aes_gcm(self, encrypted_data):
        """Dekrypter AES-GCM data"""
        aesgcm = AESGCM(self.key)
        encrypted_bytes = base64.b64decode(encrypted_data)
        nonce = encrypted_bytes[:12]
        ciphertext = encrypted_bytes[12:]
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode('utf-8')

    # METHOD 2: AES-CBC
    def encrypt_aes_cbc(self, data):
        """
        Krypter med AES-CBC

        Fordele: Veletableret standard
        Ulemper: Kræver padding, ingen indbygget integritet, langsommere
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        iv = os.urandom(16)

        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data) + padder.finalize()

        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()

        return base64.b64encode(iv + ciphertext).decode('utf-8')

    def decrypt_aes_cbc(self, encrypted_data):
        """Dekrypter AES-CBC data"""
        encrypted_bytes = base64.b64decode(encrypted_data)
        iv = encrypted_bytes[:16]
        ciphertext = encrypted_bytes[16:]

        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

        return plaintext.decode('utf-8')

    # METHOD 3: Fernet
    def encrypt_fernet(self, data):
        """
        Krypter med Fernet (AES-CBC wrapper)

        Fordele: Simpel API, indbygget HMAC
        Ulemper: Mindre fleksibel, langsommere, større ciphertext
        """
        fernet_key = base64.urlsafe_b64encode(self.key)
        f = Fernet(fernet_key)

        if isinstance(data, str):
            data = data.encode('utf-8')

        encrypted = f.encrypt(data)
        return encrypted.decode('utf-8')

    def decrypt_fernet(self, encrypted_data):
        """Dekrypter Fernet data"""
        fernet_key = base64.urlsafe_b64encode(self.key)
        f = Fernet(fernet_key)

        if isinstance(encrypted_data, str):
            encrypted_data = encrypted_data.encode('utf-8')

        decrypted = f.decrypt(encrypted_data)
        return decrypted.decode('utf-8')


def save_key(key, filename='encryption.key'):
    """Gem krypteringsnøgle til fil"""
    with open(filename, 'wb') as key_file:
        key_file.write(key)
    print(f"[OK] Key saved to {filename}")


def load_key(filename='encryption.key'):
    """Indlæs krypteringsnøgle fra fil"""
    try:
        with open(filename, 'rb') as key_file:
            key = key_file.read()
        print(f"[OK] Key loaded from {filename}")
        return key
    except FileNotFoundError:
        print(f"[WARNING] Key file not found, generating new key")
        return None
