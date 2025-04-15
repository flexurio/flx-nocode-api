use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use base64::Engine;
use rand::Rng;
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
    let ciphertext = cipher.encrypt(nonce, plaintext.as_bytes()).unwrap();

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
    base64::engine::general_purpose::STANDARD.decode(s.as_bytes()).is_ok()
}