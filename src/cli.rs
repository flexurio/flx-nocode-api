use std::io::{self, Write};

use colored::Colorize;
use rand::RngExt;

use crate::crypt::hash_password;
use crate::database::connection::initialize_database;
use crate::database::state::DbParam;
use crate::model::DbType;

/// Parse simple `--key value` pairs from CLI args.
/// Returns the value following `key` if found, otherwise `None`.
fn arg_value(args: &[String], key: &str) -> Option<String> {
    args.iter()
        .position(|a| a == key)
        .and_then(|i| args.get(i + 1).cloned())
}

/// Generate a random 4-digit numeric password.
fn generate_random_password() -> String {
    const CHARSET: &[u8] = b"0123456789";
    let mut rng = rand::rng();
    (0..4)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// Run the `reset-password` CLI command.
///
/// Resets the password for a user in `flx_users` by updating the stored hash in-place.
#[rustfmt::skip]
pub async fn reset_password(args: &[String]) -> anyhow::Result<()> {
    // ── Connect to the configured database ───────────────────────────────
    let cpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let crate::database::connection::DbInitialization {
        db_type,
        repo: db,
        ..
    } = initialize_database(cpu).await?;

    // ── Parse email from CLI args ────────────────────────────────────────
    let mut email = arg_value(args, "--email");

    // If not provided via --email, try to find a positional argument
    if email.is_none() {
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "admin" | "reset-password" => i += 1,
                "--password" | "--email" => i += 2,
                arg if !arg.starts_with('-') => {
                    email = Some(arg.to_string());
                    break;
                }
                _ => i += 1,
            }
        }
    }

    let email = match email {
        Some(e) => e,
        None => {
            // Cek apakah user 'admin' terdaftar di database
            let check_sql = match db_type {
                DbType::Postgres => "SELECT email FROM flx_users WHERE email = $1 LIMIT 1".to_string(),
                _ => "SELECT email FROM flx_users WHERE email = ? LIMIT 1".to_string(),
            };
            let check_params = vec![DbParam::Str("admin".to_string())];
            let check_rows = db.query_with_params(&check_sql, check_params).await?;
            
            if !check_rows.is_empty() {
                "admin".to_string()
            } else {
                // Query all emails from flx_users
                let sql = "SELECT email FROM flx_users".to_string();
                let rows = db.query_with_params(&sql, vec![]).await?;
                let emails: Vec<String> = rows
                    .iter()
                    .filter_map(|row| row.get("email")?.as_str().map(String::from))
                    .collect();

                if emails.is_empty() {
                    return Err(anyhow::anyhow!("No users found in database"));
                } else if emails.len() == 1 {
                    emails[0].clone()
                } else {
                    println!("\n  {} Multiple users found in database:", "ℹ".blue());
                    for (idx, e) in emails.iter().enumerate() {
                        println!("    [{}] {}", (idx + 1).to_string().cyan(), e.yellow());
                    }
                    println!();

                    let selected_email = loop {
                        print!("  Select user number (1-{}): ", emails.len());
                        io::stdout().flush()?;
                        let mut input = String::new();
                        io::stdin().read_line(&mut input)?;
                        let trimmed = input.trim();
                        if let Ok(num) = trimmed.parse::<usize>() {
                            if num >= 1 && num <= emails.len() {
                                break emails[num - 1].clone();
                            }
                        }
                        println!("  {} Invalid selection, please try again.", "❌".red());
                    };
                    selected_email
                }
            }
        }
    };

    // ── Check if the target user exists ──────────────────────────────────
    let check_sql = match db_type {
        DbType::Postgres => "SELECT 1 FROM flx_users WHERE email = $1 LIMIT 1".to_string(),
        _ => "SELECT 1 FROM flx_users WHERE email = ? LIMIT 1".to_string(),
    };
    let check_params = vec![DbParam::Str(email.clone())];
    let user_exists_rows = db.query_with_params(&check_sql, check_params).await?;
    if user_exists_rows.is_empty() {
        return Err(anyhow::anyhow!("User with email '{}' not found in database", email));
    }

    let explicit_password = arg_value(args, "--password");

    println!();
    println!("{}", "  ╔══════════════════════════════════════════╗".cyan());
    println!("{}", "  ║     Flexurio Admin Password Reset        ║".cyan());
    println!("{}", "  ╚══════════════════════════════════════════╝".cyan());
    println!();
    println!("  Target user email : {}", email.yellow());

    // ── Determine new password ───────────────────────────────────────────
    let new_password = match &explicit_password {
        Some(p) => p.clone(),
        None => generate_random_password(),
    };

    // ── Hash with Argon2 ─────────────────────────────────────────────────
    let hashed = hash_password(&new_password);
    if hashed.is_empty() {
        return Err(anyhow::anyhow!("Failed to hash the new password"));
    }

    // ── Build the UPDATE query ───────────────────────────────────────────
    // Use parameterized queries via DbRepository::query_with_params to
    // prevent SQL injection and stay cross-DB compatible.
    let sql = match db_type {
        DbType::Postgres => "UPDATE flx_users SET password = $1 WHERE email = $2".to_string(),
        _ => "UPDATE flx_users SET password = ? WHERE email = ?".to_string(),
    };

    let params = vec![
        DbParam::Str(hashed),
        DbParam::Str(email.clone()),
    ];

    // ── Execute ──────────────────────────────────────────────────────────
    match db.query_with_params(&sql, params).await {
        Ok(_) => {
            println!();
            println!("{}", "  ╔══════════════════════════════════════════╗".green());
            println!("{}", "  ║        Password Reset Successful!        ║".green());
            println!("{}", "  ╚══════════════════════════════════════════╝".green());
            println!();
            println!("  Email    : {}", email.yellow());
            if explicit_password.is_none() {
                println!("  Password : {}", new_password.yellow().bold());
                println!();
                println!(
                    "  {}",
                    "⚠  Save this password now — it will not be shown again.".red().bold()
                );
            } else {
                println!("  Password : {}", "(as specified)".yellow());
            }
            println!();
        }
        Err(e) => {
            eprintln!();
            eprintln!("  {} {}", "Error:".red().bold(), e);
            eprintln!("  Make sure the database is accessible and the user exists.");
            return Err(anyhow::anyhow!("Failed to reset password: {}", e));
        }
    }

    Ok(())
}
