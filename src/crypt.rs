use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use argon2::{
    password_hash::{phc::PasswordHash, PasswordHasher, PasswordVerifier},
    Argon2,
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
    let Ok(gcm_key) = Key::<Aes256Gcm>::try_from(key_hash.as_slice()) else {
        eprintln!("Encryption failed: invalid key length");
        return String::new();
    };
    let cipher = Aes256Gcm::new(&gcm_key);

    // Generate IV (nonce)
    let mut iv = [0u8; 12];
    rand::rng().fill(&mut iv);
    let Ok(nonce) = Nonce::try_from(iv.as_slice()) else {
        eprintln!("Encryption failed: invalid nonce length");
        return String::new();
    };

    // Enkripsi plaintext
    let ciphertext = match cipher.encrypt(&nonce, plaintext.as_bytes()) {
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
    let Ok(gcm_key) = Key::<Aes256Gcm>::try_from(key_hash.as_slice()) else {
        return "Gagal dekripsi".to_string();
    };
    let cipher = Aes256Gcm::new(&gcm_key);

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
    let Ok(nonce) = Nonce::try_from(iv) else {
        return "Data terenkripsi tidak valid".to_string();
    };

    // Dekripsi
    match cipher.decrypt(&nonce, ciphertext) {
        Ok(plaintext) => match String::from_utf8(plaintext) {
            Ok(text) => text,
            Err(_) => "Gagal konversi ke string".to_string(),
        },
        Err(_) => "Gagal dekripsi".to_string(),
    }
}

/// Hash sebuah password plaintext dengan Argon2id (one-way, salt acak per-hash).
///
/// Gunakan ini untuk kredensial autentikasi (mis. kolom `flx_users.password`).
/// Berbeda dengan [`encrypt`] yang reversible (untuk kolom PII yang harus bisa
/// dibaca kembali), hash password TIDAK dapat didekripsi — verifikasi dilakukan
/// dengan [`verify_password`]. Mengembalikan string PHC (`$argon2id$...`).
/// String kosong dikembalikan bila hashing gagal (caller harus memperlakukan
/// nilai kosong sebagai kegagalan, bukan sebagai hash valid).
pub fn hash_password(plaintext: &str) -> String {
    match Argon2::default().hash_password(plaintext.as_bytes()) {
        Ok(hash) => hash.to_string(),
        Err(e) => {
            eprintln!("Password hashing failed: {}", e);
            String::new()
        }
    }
}

/// Verifikasi password plaintext terhadap hash PHC Argon2 (constant-time).
/// Mengembalikan `false` bila hash tidak valid/tidak cocok.
pub fn verify_password(plaintext: &str, phc_hash: &str) -> bool {
    match PasswordHash::new(phc_hash) {
        Ok(parsed) => Argon2::default()
            .verify_password(plaintext.as_bytes(), &parsed)
            .is_ok(),
        Err(_) => false,
    }
}

/// True bila string tampak sebagai hash Argon2 PHC (mis. `$argon2id$...`).
/// Dipakai untuk membedakan kredensial yang sudah di-hash dari format lama
/// (AES terenkripsi) demi migrasi mulus saat login.
pub fn is_argon2_hash(s: &str) -> bool {
    s.starts_with("$argon2")
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

    // --- password hashing (Argon2) ---

    #[test]
    fn test_hash_password_verifies() {
        let hash = hash_password("s3cr3t-pass");
        assert!(is_argon2_hash(&hash), "hash should be PHC argon2 format");
        assert!(verify_password("s3cr3t-pass", &hash));
        assert!(!verify_password("wrong-pass", &hash));
    }

    #[test]
    fn test_hash_password_uses_random_salt() {
        let h1 = hash_password("same");
        let h2 = hash_password("same");
        assert_ne!(h1, h2, "random salt should yield different hashes");
        assert!(verify_password("same", &h1));
        assert!(verify_password("same", &h2));
    }

    #[test]
    fn test_verify_password_rejects_non_hash() {
        // Legacy AES ciphertext / arbitrary string must not be accepted as a valid hash.
        assert!(!verify_password("anything", "not-a-phc-hash"));
        assert!(!verify_password("anything", ""));
    }

    #[test]
    fn test_is_argon2_hash_discriminates_from_aes() {
        let aes = encrypt("k".to_string(), "data".to_string());
        assert!(!is_argon2_hash(&aes), "AES base64 must not look like an argon2 hash");
        assert!(is_argon2_hash(&hash_password("x")));
    }
}
