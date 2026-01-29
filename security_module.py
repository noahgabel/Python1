"""
Security Module - Kryptografi
Indeholder 3 forskellige AES krypteringsmetoder
"""

import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet
import base64


class SecurityManager:
    """
    Håndterer kryptering og dekryptering af data
    """

    def __init__(self, key=None):
        """
        Initialiserer med en 32-byte AES nøgle.
        Hvis ingen nøgle gives, genereres en ny.
        """
        if key is None:
            self.key = os.urandom(32)  # 256-bit AES key
        else:
            self.key = key

    def get_key(self):
        """Returnerer nøglen (til lagring)"""
        return self.key

    # METHOD 1: AES-GCM
    def encrypt_aes_gcm(self, data):
        """
        Krypter med AES-GCM (Galois/Counter Mode)

        Fordele:
        - AEAD (Authenticated Encryption with Associated Data)
        - Indbygget integritet og autenticitet
        - Ingen padding nødvendig
        - Hurtig (hardware acceleration)
        - Modstandsdygtig mod manipulation

        Ulemper:
        - Nonce skal være unik for hver kryptering
        """
        aesgcm = AESGCM(self.key)
        nonce = os.urandom(12)  # 96-bit nonce

        if isinstance(data, str):
            data = data.encode('utf-8')

        ciphertext = aesgcm.encrypt(nonce, data, None)

        # Returner nonce + ciphertext
        return base64.b64encode(nonce + ciphertext).decode('utf-8')

    def decrypt_aes_gcm(self, encrypted_data):
        """Dekrypter AES-GCM krypteret data"""
        aesgcm = AESGCM(self.key)

        encrypted_bytes = base64.b64decode(encrypted_data)
        nonce = encrypted_bytes[:12]
        ciphertext = encrypted_bytes[12:]

        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return plaintext.decode('utf-8')

    # METHOD 2: AES-CBC
    def encrypt_aes_cbc(self, data):
        """
        Krypter med AES-CBC (Cipher Block Chaining)

        Fordele:
        - Veletableret standard
        - God sikkerhed når brugt korrekt

        Ulemper:
        - Kræver padding (PKCS7)
        - Ingen indbygget integritet (skal bruge HMAC separat)
        - Risiko for padding oracle attacks
        - Langsommere end GCM
        """
        if isinstance(data, str):
            data = data.encode('utf-8')

        # Generer random IV (Initialization Vector)
        iv = os.urandom(16)

        # Padding til blokstørrelse (128 bit / 16 bytes)
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(data) + padder.finalize()

        # Krypter
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()

        # Returner IV + ciphertext
        return base64.b64encode(iv + ciphertext).decode('utf-8')

    def decrypt_aes_cbc(self, encrypted_data):
        """Dekrypter AES-CBC krypteret data"""
        encrypted_bytes = base64.b64decode(encrypted_data)
        iv = encrypted_bytes[:16]
        ciphertext = encrypted_bytes[16:]

        # Dekrypter
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()

        # Fjern padding
        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()

        return plaintext.decode('utf-8')

    # METHOD 3: Fernet (AES-CBC wrapper)
    def encrypt_fernet(self, data):
        """
        Krypter med Fernet (high-level AES-CBC wrapper)

        Fordele:
        - Meget simpel API
        - Indbygget integritet (HMAC)
        - Timestamp support
        - God til simple use cases

        Ulemper:
        - Mindre fleksibel
        - Langsommere end GCM
        - Større ciphertext (overhead)
        """
        # Fernet kræver 32-byte URL-safe base64-encoded key
        fernet_key = base64.urlsafe_b64encode(self.key)
        f = Fernet(fernet_key)

        if isinstance(data, str):
            data = data.encode('utf-8')

        encrypted = f.encrypt(data)
        return encrypted.decode('utf-8')

    def decrypt_fernet(self, encrypted_data):
        """Dekrypter Fernet krypteret data"""
        fernet_key = base64.urlsafe_b64encode(self.key)
        f = Fernet(fernet_key)

        if isinstance(encrypted_data, str):
            encrypted_data = encrypted_data.encode('utf-8')

        decrypted = f.decrypt(encrypted_data)
        return decrypted.decode('utf-8')


# Gem nøgle til fil
def save_key(key, filename='encryption.key'):
    """Gem krypteringsnøgle til fil"""
    with open(filename, 'wb') as key_file:
        key_file.write(key)
    print(f"[OK] Key saved to {filename}")


# Indlæs nøgle fra fil
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
