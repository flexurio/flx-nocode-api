use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use base64::Engine;
use rand::RngExt;
use sha2::{Digest, Sha256};

/// Fungsi untuk mengenkripsi data dengan AES-256-GCM
pub fn encrypt(key: String, plaintext: String) -> String {
    // Hash kunci sembarang panjang jadi 32 byte dengan SHA-256
    let mut hasher = Sha256::default();
    hasher.update(key.as_bytes());
    let key_hash = hasher.finalize();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_hash));

    // Generate IV (nonce)
    let mut iv = [0u8; 12];
    rand::rng().fill(&mut iv);
    let nonce = Nonce::from_slice(&iv);

    // Enkripsi plaintext
    let ciphertext = match cipher.encrypt(nonce, plaintext.as_bytes()) {
        Ok(encrypted) => encrypted,
        Err(e) => {
            eprintln!("Encryption failed: {}", e);
            return String::new();
        }
    };

    // Gabungkan IV dan ciphertext
    let mut encrypted_data = Vec::new();
    encrypted_data.extend_from_slice(&iv);
    encrypted_data.extend_from_slice(&ciphertext);

    base64::engine::general_purpose::STANDARD.encode(encrypted_data)
}

/// Fungsi untuk mendekripsi data dengan AES-256-GCM
pub fn decrypt(key: String, encrypted_string: String) -> String {
    // Hash kunci sembarang panjang jadi 32 byte dengan SHA-256
    let mut hasher = Sha256::default();
    hasher.update(key.as_bytes());
    let key_hash = hasher.finalize();
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key_hash));

    // Decode dari Base64
    let encrypted_data =
        match base64::engine::general_purpose::STANDARD.decode(encrypted_string.as_bytes()) {
            Ok(data) => data,
            Err(_) => return "Gagal decode Base64".to_string(),
        };

    // Pisahkan IV dan ciphertext
    if encrypted_data.len() < 12 {
        return "Data terenkripsi tidak valid".to_string();
    }
    let (iv, ciphertext) = encrypted_data.split_at(12);
    let nonce = Nonce::from_slice(iv);

    // Dekripsi
    match cipher.decrypt(nonce, ciphertext) {
        Ok(plaintext) => match String::from_utf8(plaintext) {
            Ok(text) => text,
            Err(_) => "Gagal konversi ke string".to_string(),
        },
        Err(_) => "Gagal dekripsi".to_string(),
    }
}

/// Fungsi untuk check apakah string sudah di enkripsi atau belum
pub fn is_encrypted_string(s: &str) -> bool {
    // Cek apakah panjang string lebih dari 12 karakter
    if s.len() < 12 {
        return false;
    }

    // Cek apakah string dapat didecode dari Base64
    base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = "my_secret_key".to_string();
        let plaintext = "Hello, World!".to_string();
        let encrypted = encrypt(key.clone(), plaintext.clone());
        assert!(!encrypted.is_empty());
        let decrypted = decrypt(key, encrypted);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_empty_string_roundtrip() {
        let key = "test_key".to_string();
        let plaintext = "".to_string();
        let encrypted = encrypt(key.clone(), plaintext.clone());
        let decrypted = decrypt(key, encrypted);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encrypt_different_keys_produce_different_ciphertext() {
        let plaintext = "same plaintext".to_string();
        let enc1 = encrypt("key1".to_string(), plaintext.clone());
        let enc2 = encrypt("key2".to_string(), plaintext.clone());
        assert_ne!(enc1, enc2);
    }

    #[test]
    fn test_decrypt_wrong_key_does_not_match_original() {
        let plaintext = "secret data".to_string();
        let encrypted = encrypt("correct_key".to_string(), plaintext.clone());
        let result = decrypt("wrong_key".to_string(), encrypted);
        assert_ne!(result, plaintext);
    }

    #[test]
    fn test_encrypt_produces_valid_base64_output() {
        let encrypted = encrypt("key".to_string(), "value".to_string());
        use base64::Engine;
        assert!(base64::engine::general_purpose::STANDARD
            .decode(encrypted.as_bytes())
            .is_ok());
    }

    #[test]
    fn test_encrypt_same_plaintext_random_nonce_different_output() {
        // Random nonce means the same plaintext should produce different ciphertext each call
        let enc1 = encrypt("key".to_string(), "data".to_string());
        let enc2 = encrypt("key".to_string(), "data".to_string());
        assert_ne!(enc1, enc2, "Two encryptions should differ due to random nonce");
    }

    #[test]
    fn test_encrypt_decrypt_unicode_content() {
        let key = "unicode_key".to_string();
        let plaintext = "Indonesian: selamat pagi 🌅".to_string();
        let encrypted = encrypt(key.clone(), plaintext.clone());
        let decrypted = decrypt(key, encrypted);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decrypt_invalid_base64_returns_error_message() {
        let result = decrypt("key".to_string(), "not_valid_base64!!!".to_string());
        assert!(result.contains("Gagal"), "Expected Indonesian error message, got: {}", result);
    }

    #[test]
    fn test_decrypt_too_short_data_returns_error_message() {
        // Base64 of something shorter than 12 bytes
        use base64::Engine;
        let short = base64::engine::general_purpose::STANDARD.encode(b"short");
        let result = decrypt("key".to_string(), short);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_is_encrypted_string_valid_encrypted_value() {
        let encrypted = encrypt("key".to_string(), "data".to_string());
        assert!(is_encrypted_string(&encrypted));
    }

    #[test]
    fn test_is_encrypted_string_too_short() {
        assert!(!is_encrypted_string("short"));
        assert!(!is_encrypted_string(""));
    }

    #[test]
    fn test_is_encrypted_string_invalid_chars() {
        // Contains characters outside base64 alphabet
        assert!(!is_encrypted_string("not!@#$%^&*()encrypted"));
    }

    #[test]
    fn test_encrypt_large_payload() {
        let key = "big_key".to_string();
        let plaintext = "A".repeat(10_000);
        let encrypted = encrypt(key.clone(), plaintext.clone());
        let decrypted = decrypt(key, encrypted);
        assert_eq!(decrypted, plaintext);
    }
}
